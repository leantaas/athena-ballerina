#!/usr/bin/env python

import argparse
from argparse import ArgumentParser
from collections import namedtuple
from hashlib import sha256

from pathlib import PosixPath
from typing import Iterable, List, Optional, Set, T, Tuple, Dict

import boto3
import logging

from aws_helper import AthenaInfo, S3Info, executor
from version import __version__

Migration = namedtuple('Migration', 'migration_id up_digest down_digest up down')
FILE_DELIM = ':'

logging.basicConfig(format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
log = logging.getLogger('ballerina')
log.setLevel(logging.INFO)


def get_migration_id(file_name: str) -> int:
    try:
        return int(file_name[:file_name.index('_')])
    except ValueError:
        log.error(f'File "{file_name}" is not of pattern "(id)_up.sql" or "(id)_down.sql"')
        exit(3)


def get_max_migration_id(filenames: List[str]) -> int:
    return max(
        get_migration_id(file_name)
        for file_name in filenames
    )


def get_migration_files_filtered(directory: PosixPath) -> List[str]:
    return [file.name for file in directory.iterdir() if file.is_file() and file.name.lower().endswith('.sql')]


def assert_all_migrations_present(directory: PosixPath) -> None:
    filenames: List[str] = get_migration_files_filtered(directory)
    if not filenames:
        log.error(f'Migrations folder {directory} is empty. Exiting gracefully!')
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
        log.error('ERROR: Extra files not of pattern "(id)_up.sql" or "(id)_down.sql": %s', ','.join(extra_files))
        exit(3)


def parse_migrations(directory: PosixPath, params: Dict[str, str]) -> List[Migration]:
    filenames: List[str] = get_migration_files_filtered(directory)
    max_migration_id: int = get_max_migration_id(filenames)

    migrations: List[Migration] = [
        parse_migration(directory, migration_id, params)
        for migration_id in range(1, max_migration_id + 1)
    ]

    return migrations


def parse_migration(directory: PosixPath, migration_id: int, params: Dict[str, str]) -> Migration:
    up_file: PosixPath = directory.joinpath(f'{migration_id}_up.sql')
    down_file: PosixPath = directory.joinpath(f'{migration_id}_down.sql')

    with open(str(up_file)) as up_fp, open(str(down_file)) as down_fp:
        up = up_fp.read().format(**params)
        down = down_fp.read().format(**params)
        migration = Migration(
            migration_id=migration_id,
            up_digest=digest(up),
            down_digest=digest(down),
            up=up,
            down=down
        )
        return migration


def get_migration_prefix(prefix, migration):
    return prefix + FILE_DELIM.join([str(x) for x in migration[:3]])


def parse_migration_prefix(prefix, filename) -> List[str]:
    if filename.startswith(prefix):
        filename = filename[len(prefix):]
    if filename.endswith('_up.sql'):
        filename = filename[:-len('_up.sql')]
    if filename.endswith('_down.sql'):
        filename = filename[:-len('_down.sql')]
    migration_parts = filename.split(FILE_DELIM)
    migration_parts[0] = int(migration_parts[0])
    assert len(migration_parts) == 3, f'Invalid file found in bucket: {prefix}/{filename}'
    return migration_parts


def digest(s: str) -> str:
    return sha256(s.encode('utf-8')).hexdigest()


def main(migrations_directory: PosixPath, dbname: str, migration_bucket: str, migration_prefix: str,
         staging_uri: Optional[str], work_group: Optional[str], params: Dict[str, str], auto_apply_down: bool,
         auto_clean_up: bool, boto_kwargs: Dict[str, str]) -> None:
    assert_all_migrations_present(migrations_directory)

    sess = boto3.Session(**boto_kwargs)
    s3 = S3Info(sess.client('s3'), migration_bucket, migration_prefix)
    athena = AthenaInfo(sess.client('athena'), dbname, staging_uri, work_group, s3.client if auto_clean_up else None)

    migrations_from_db: List[Migration] = sorted(get_db_migration_digests(s3))
    migrations_from_filesystem: List[Migration] = sorted(parse_migrations(migrations_directory, params))

    old_branch, new_branch = get_diff(migrations_from_db, migrations_from_filesystem)

    if old_branch:
        if auto_apply_down:
            old_branch = [fill_db_migration(s3, m) for m in old_branch]
            unapply_all(s3, athena, old_branch)
        else:
            log.error('\nError:')
            log.error('    -a / --auto_apply_down flag is set to false')
            log.error('    Failing migrations.')
            log.error(f'    Failed at migration number: {old_branch[0].migration_id}')
            exit(5)

    apply_all(s3, athena, new_branch)
    executor.shutdown(wait=True)


def apply_all(s3: S3Info, athena: AthenaInfo, migrations) -> None:
    assert sorted(migrations) == migrations, 'Migrations must be applied in ascending order'
    for migration in migrations:
        apply_up(s3, athena, migration)


def unapply_all(s3: S3Info, athena: AthenaInfo, migrations) -> None:
    assert sorted(migrations, reverse=True) == migrations, 'Migrations must be unapplied in descending order'
    for migration in migrations:
        apply_down(s3, athena, migration)


def apply_up(s3: S3Info, athena: AthenaInfo, migration: Migration) -> None:
    log.info(f'Applying {migration.migration_id}_up.sql')
    log.debug(migration.up)
    athena.execute_many(migration.up)
    file_prefix = get_migration_prefix(s3.prefix, migration)
    s3.write(f'{file_prefix}_up.sql', migration.up)
    s3.write(f'{file_prefix}_down.sql', migration.down)


def apply_down(s3: S3Info, athena: AthenaInfo, migration: Migration) -> None:
    log.info(f'Applying {migration.migration_id}_down.sql')
    log.debug(migration.down)
    file_prefix = get_migration_prefix(s3.prefix, migration)
    athena.execute_many(migration.down)
    s3.delete(f'{file_prefix}_up.sql')
    s3.delete(f'{file_prefix}_down.sql')


def _parse_args() -> dict:
    parser = ArgumentParser()
    parser.add_argument('migrations_directory', help='Path to directory containing migrations')
    parser.add_argument('-m', '--migration_uri', help='S3 Migration Dir. (i.e: "s3://my-bucket/path/to/folder/")')
    parser.add_argument('-s', '--staging_uri', help='Athena Staging dir URI (i.e: "s3://my-bucket/path/to/folder/")')
    parser.add_argument('-w', '--work_group', help='Athena Work Group')
    parser.add_argument('-d', '--dbname', default='default')
    parser.add_argument('-D', '--delim', default='/', help='Delimiter used in S3 bucket.')
    parser.add_argument(
        '-p',
        '--param',
        action='append',
        nargs=2,
        help='Parameter that can be formatted into the migration file. For example if "-p KEY VAL" gets passed in CLI, '
             'and in the migration file there is a python-formatted string like "LOCATION s3://{KEY}/", it will be '
             'formatted to  "LOCATION s3://VAL/"'
    )

    parser.add_argument('--aws_access_key_id', help='AWS Access Key for Boto3')
    parser.add_argument('--aws_secret_access_key', help='AWS Access Secret for Boto3')
    parser.add_argument('--aws_session_token', help='AWS Access Session Token for Boto3')
    parser.add_argument('--aws_region_name', help='AWS Region Name for Boto3')
    parser.add_argument('--aws_profile_name', help='AWS Profile Name for Boto3')

    parser.add_argument('-a', '--auto_apply_down', default=True, type=str2bool,
                        help="Accepts True/False, default is True")

    parser.add_argument(
        '-c',
        '--auto_clean_up',
        default=True,
        type=str2bool,
        help='Should Athena Queries be clean-up from S3 OutputLocation? Accepts True/False.'
    )

    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {version}'.format(version=__version__))

    parser.add_argument('-n', '--noisy', '--verbose', default=False, type=str2bool,
                        help='Print migrations to console (True/False)')

    args = parser.parse_args()
    log.info(args)

    # S3 is a flat structure, so it's easy to specify paths incorrectly. However, the result would be catastrophic.
    assert args.migration_uri.endswith(args.delim), f'The specified URI "{args.migration_uri}" does not end with ' \
                                                    f'the specified delimiter "{args.delim}"'
    migration_bucket, migration_prefix = S3Info.parse_s3_url(args.migration_uri)
    assert not migration_prefix.startswith(args.delim), 'S3 Paths should not start with a delimiter'

    migrations_directory = _get_migrations_directory(args.migrations_directory)

    boto_kwargs = dict(
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
        aws_session_token=args.aws_session_token,
        region_name=args.aws_region_name,
        profile_name=args.aws_profile_name,
    )
    params = dict(args.param) if args.param else {}
    return dict(
        migrations_directory=migrations_directory,
        dbname=args.dbname,

        migration_bucket=migration_bucket,
        migration_prefix=migration_prefix,
        staging_uri=args.staging_uri,
        work_group=args.work_group,

        params=params,

        boto_kwargs=boto_kwargs,
        auto_apply_down=args.auto_apply_down,
        auto_clean_up=args.auto_clean_up
    )


def _get_migrations_directory(pathname: str) -> PosixPath:
    migrations_directory = PosixPath(pathname).absolute()

    if not migrations_directory.is_dir():
        log.error(f'{migrations_directory.as_posix()} is not a directory')
        exit(1)
    else:
        return migrations_directory


def first(xs: Iterable[T]) -> Optional[T]:
    try:
        return next(iter(xs))
    except StopIteration:
        return None


def get_db_migration_digests(s3: S3Info) -> List[Migration]:
    migrations = set()
    continuation_token = None
    while True:
        continuation_kwargs = dict(ContinuationToken=continuation_token) if continuation_token else {}
        response = s3.client.list_objects_v2(Bucket=s3.bucket, Prefix=s3.prefix, **continuation_kwargs)

        if 'Contents' in response:
            migrations.update([
                Migration(*parse_migration_prefix(s3.prefix, r['Key']), None, None) for r in response['Contents']
            ])

        if 'IsTruncated' in response and response['IsTruncated'] and 'NextContinuationToken' in response:
            continuation_token = response['NextContinuationToken']
        else:
            break

    return list(migrations)


def fill_db_migration(s3: S3Info, migration: Migration, down_only=True) -> Migration:
    down = s3.read(f'{get_migration_prefix(s3.prefix, migration)}_down.sql')
    up = None
    if not down_only:
        up = s3.read(f'{get_migration_prefix(s3.prefix, migration)}_up.sql')
    return Migration(migration.migration_id, migration.up_digest, migration.down_digest, up, down)


def get_diff(db_migrations: List[Migration], file_system_migrations: List[Migration]) -> \
        Tuple[List[Migration], List[Migration]]:

    first_divergence: Optional[Migration] = first(
        db_migration
        for db_migration, fs_migration in zip(db_migrations, file_system_migrations)
        if db_migration.up_digest != fs_migration.up_digest
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


def cli():
    main(**_parse_args())


if __name__ == '__main__':
    cli()
