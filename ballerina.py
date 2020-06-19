#!/usr/bin/env python

import argparse
from argparse import ArgumentParser
from collections import namedtuple
from hashlib import sha256
from io import StringIO
from pathlib import PosixPath
from typing import Iterable, List, Optional, Set, T, Tuple
from urllib.parse import urlparse

import boto3

import athena_helper
from version import __version__

DBParams = namedtuple('DBParams', 'user password host port database')
Migration = namedtuple('Migration', 'migration_id up down')


class S3Info(namedtuple('S3Conn', 'client bucket prefix')):
    S3_DELIM = ':'

    def migration_prefix(self, migration):
        return self.prefix + self.__class__.S3_DELIM.join(
            [migration.migration_id, digest(migration.up), digest(migration.down)]
        )

    def parse_migration_prefix(self, filename):
        if filename.startswith(self.prefix):
            filename = filename[len(self.prefix):]
        if filename.endswith('.sql'):
            filename = filename[:-len('.sql')]
        return filename.split(self.__class__.S3_DELIM)

    @staticmethod
    def parse_s3_url(s3url):
        parsed_url = urlparse(s3url)
        return parsed_url.netloc, parsed_url.path.lstrip('/')


def get_migration_id(file_name: str) -> int:
    try:
        return int(file_name[:file_name.index('_')])
    except ValueError:
        print(f'ERROR: File "{file_name}" is not of pattern "(id)_up.sql" or "(id)_down.sql"')
        exit(3)


def get_max_migration_id(filenames: List[str]) -> int:
    return max(
        get_migration_id(file_name)
        for file_name in filenames
    )


def get_migration_files_filtered(dir: PosixPath) -> List[str]:
    return [str(file) for file in dir.iterdir() if file.is_file() and str(file).lower().endswith('.sql')]


def assert_all_migrations_present(dir: PosixPath) -> None:
    filenames: List[str] = get_migration_files_filtered(dir)
    if not filenames:
        print(f'Migrations folder {dir} is empty. Exiting gracefully!')
        exit(0)

    max_migration_id = get_max_migration_id(filenames)

    for migration_id in range(1, max_migration_id + 1):
        # todo - assertions can be ignored...?
        assert f'{migration_id}_up.sql' in filenames, f'Migration {migration_id} missing ups'
        assert f'{migration_id}_down.sql' in filenames, f'Migration {migration_id} missing downs'

    extra_files: Set[str] = (
            set(filenames)
            - {f'{m_id}_up.sql' for m_id in range(1, max_migration_id + 1)}
            - {f'{m_id}_down.sql' for m_id in range(1, max_migration_id + 1)}
    )

    if extra_files:
        print('ERROR: Extra files not of pattern "(id)_up.sql" or "(id)_down.sql": ')
        print(*extra_files, sep='\n')
        exit(3)


def parse_migrations(dir: PosixPath) -> List[Migration]:
    filenames: List[str] = get_migration_files_filtered(dir)
    max_migration_id: int = get_max_migration_id(filenames)

    migrations: List[Migration] = [
        parse_migration(dir, migration_id)
        for migration_id in range(1, max_migration_id + 1)
    ]

    return migrations


def parse_migration(dir: PosixPath, migration_id: int) -> Migration:
    up_file: PosixPath = dir.joinpath(f'{migration_id}_up.sql')
    down_file: PosixPath = dir.joinpath(f'{migration_id}_down.sql')

    with open(up_file) as up_fp, open(down_file) as down_fp:
        migration = Migration(
            migration_id=migration_id,
            up=up_fp.read(),
            down=down_fp.read()
        )
        return migration


def main() -> None:
    migrations_directory, bucket, path, session_kwargs, auto_apply_down = _parse_args()

    assert_all_migrations_present(migrations_directory)

    sess = boto3.Session(**session_kwargs)
    s3 = S3Info(sess.client('s3'), bucket, path)
    athena = sess.client('athena')

    migrations_from_db: List[Migration] = sorted(get_db_migrations(s3))
    migrations_from_filesystem: List[Migration] = sorted(parse_migrations(migrations_directory))

    old_branch, new_branch = get_diff(migrations_from_db, migrations_from_filesystem)

    if old_branch:
        if auto_apply_down:
            unapply_all(s3, athena, old_branch)
        else:
            print('\nError:')
            print('    -a / --auto_apply_down flag is set to false')
            print('    Failing migrations.')
            print(f'    Failed at migration number: {old_branch[0].migration_id}')
            exit(5)

        apply_all(s3, athena, new_branch)


def apply_all(s3: S3Info, athena, migrations) -> None:
    assert (
        sorted(migrations) == migrations,
        'Migrations must be applied in ascending order'
    )
    for migration in migrations:
        apply_up(s3, athena, migration)


def unapply_all(s3: S3Info, athena, migrations) -> None:
    assert (
        sorted(migrations, reverse=True) == migrations,
        'Migrations must be unapplied in descending order'
    )
    for migration in migrations:
        apply_down(s3, athena, migration)


def apply_up(s3: S3Info, athena, migration: Migration) -> None:
    print(migration.migration_id, migration.up, end='\n' * 2)
    athena_helper.query_multiple_and_wait(athena, migration.up)
    file_prefix = s3.migration_prefix(migration)

    s3.client.upload_fileobj(
        Fileobj=StringIO(migration.up),
        Bucket=s3.bucket,
        Key=f'{file_prefix}_up.sql'
    )
    s3.client.upload_fileobj(
        Fileobj=StringIO(migration.down),
        Bucket=s3.bucket,
        Key=f'{file_prefix}_down.sql'
    )


def apply_down(s3, athena, migration: Migration) -> None:
    print(migration.migration_id, migration.down, end='\n' * 2)
    file_prefix = s3.migration_prefix(migration)
    athena_helper.query_multiple_and_wait(athena, migration.down)
    s3.client.delete_object(Bucket=s3.bucket, Key=f'{file_prefix}_up.sql')
    s3.client.delete_object(Bucket=s3.bucket, Key=f'{file_prefix}_down.sql')


def _parse_args() -> Tuple[PosixPath, str, str, dict, bool]:
    parser = ArgumentParser()
    parser.add_argument('migrations_directory', help='Path to directory containing migrations')
    parser.add_argument('-u', '--uri', help='S3 URI (i.e: "s3://my-bucket/path/to/migrations/folder/"')
    parser.add_argument('-d', '--dbname', default='default')
    parser.add_argument('--delim', default='/', help='Delimiter used in S3 bucket. The specified prefix must end '
                                                     'with this character.')
    parser.add_argument('--aws_access_key_id', help='AWS Access Key for Boto3')
    parser.add_argument('--aws_secret_access_key', help='AWS Access Secret for Boto3')
    parser.add_argument('--aws_session_token', help='AWS Access Session Token for Boto3')
    parser.add_argument('--aws_region_name', help='AWS Region Name for Boto3')
    parser.add_argument('--aws_profile_name', help='AWS Profile Name for Boto3')

    parser.add_argument('-a', '--auto_apply_down', default=True, type=str2bool,
                        help="Accepts True/False, default is True")

    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {version}'.format(version=__version__))

    args = parser.parse_args()
    print(args)

    # S3 is a flat structure, so it's easy to specify paths incorrectly. Also, the result would be catastrophic.
    assert args.uri.endswith(args.delim), f'The specified URI "{args.uri}" does not end with the specified delimiter ' \
                                          f'"{args.delim}"'
    bucket, prefix = S3Info.parse_s3_url(args.uri)
    assert not prefix.startswith(args.delim), 'S3 Paths should not start with a delimiter'

    migrations_directory = _get_migrations_directory(args.migrations_directory)

    session_kwargs = dict(
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
        aws_session_token=args.aws_session_token,
        region_name=args.aws_region_name,
        profile_name=args.aws_profile_name,
    )
    return migrations_directory, bucket, prefix, session_kwargs, args.auto_apply_down


def _get_migrations_directory(pathname: str) -> PosixPath:
    migrations_directory = PosixPath(pathname).absolute()

    if not migrations_directory.is_dir():
        print(f'ERROR: {migrations_directory.as_posix()} is not a directory')
        exit(1)
    else:
        return migrations_directory


def digest(s: str) -> str:
    return sha256(s.encode('utf-8')).hexdigest()


def first(xs: Iterable[T]) -> Optional[T]:
    try:
        return next(iter(xs))
    except StopIteration:
        return None


def get_db_migrations(s3: S3Info) -> List[Migration]:
    response = s3.client.list_objects_v2(Bucket=s3.bucket, Prefix=s3.prefix)
    return [Migration(*s3.parse_migration_prefix(r['Key'])) for r in response['Contents']]


def get_diff(db_migrations: List[Migration], file_system_migrations: List[Migration]) -> \
        Tuple[List[Migration], List[Migration]]:

    first_divergence: Optional[Migration] = first(
        db_migration
        for db_migration, fs_migration in zip(db_migrations, file_system_migrations)
        if digest(db_migration.up) != digest(fs_migration.up)
    )

    if first_divergence:
        old_branch = sorted([m for m in db_migrations if m.migration_id >= first_divergence.migration_id], reverse=True)
        new_branch = sorted([m for m in file_system_migrations if m.migration_id >= first_divergence.migration_id])
        return old_branch, new_branch
    else:
        old_branch = []
        max_old_id = 0 if not db_migrations else max(m.migration_id for m in db_migrations)
        new_branch = sorted([m for m in file_system_migrations if m.migration_id > max_old_id])
        return old_branch, new_branch


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


if __name__ == '__main__':
    main()
