from collections import namedtuple

from botocore.client import BaseClient


class AthenaQueryError(ValueError):
    pass


def query_multiple_and_wait(athena: BaseClient, sql: str):
    pass


def query_and_wait(athena: BaseClient, sql: str):
    pass
