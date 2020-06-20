import time
from collections import namedtuple
from concurrent.futures.thread import ThreadPoolExecutor
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse


class AthenaQueryError(ValueError):
    pass


RUNNING = 'RUNNING'
SUCCESS = 'SUCCEEDED'
FAILURE = 'FAILED'


executor = ThreadPoolExecutor(max_workers=3)


def keys_in_nested_dict(dictionary, *args):
    subdict = dictionary
    for key in args:
        if key not in subdict:
            return False
        subdict = subdict[key]
    return True


class AthenaInfo(namedtuple('AthenaInfo', 'client database output_uri work_group cleanup_client')):
    HEARTBEAT = 0.5

    def execute_many(self, queries):
        """Attempts to execute multiple queries in sequence by splitting on semi-colons"""
        parsed_queries = [q.strip('\n ;') for q in queries.split(';')]
        for q in parsed_queries:
            if q:
                self.execute(q)

    def execute(self, query):
        """
        Executes a single query with AWS Athena. If an s3 cleanup_client is provided a thread will be dispatched to
        """
        start_query_params = dict(QueryString=query)
        if self.database:
            start_query_params['QueryExecutionContext'] = dict(Database=self.database)
        if self.output_uri:
            start_query_params['ResultConfiguration'] = dict(OutputLocation=self.output_uri)
        if self.work_group:
            start_query_params['WorkGroup'] = self.work_group

        query_exec_id = self.client.start_query_execution(**start_query_params)['QueryExecutionId']

        state = RUNNING
        response = {}
        while state == RUNNING:
            response = self.client.get_query_execution(QueryExecutionId=query_exec_id)
            if keys_in_nested_dict(response, 'QueryExecution', 'Status', 'State'):
                state = response['QueryExecution']['Status']['State']
                if state == FAILURE:
                    failure_reason = 'Athena failed to execute query'
                    if 'Query' in response:
                        failure_reason += f": {response['Query']}"
                    if 'StateChangeReason' in response['QueryExecution']['Status']:
                        failure_reason += f". Reason: {response['QueryExecution']['Status']}."
                    raise AthenaQueryError(failure_reason)

            if state == RUNNING:
                time.sleep(self.__class__.HEARTBEAT)

        if self.cleanup_client and keys_in_nested_dict(response, 'ResultConfiguration', 'OutputLocation'):
            s3_uri = response['ResultConfiguration']['OutputLocation']
            executor.submit(self.cleanup_client, s3_uri)
            executor.submit(self.cleanup_client, s3_uri + '.metadata')

    def cleanup(self, s3_uri):
        bucket, key = S3Info.parse_s3_url(s3_uri)
        self.cleanup_client.delete_object(Bucket=bucket, Key=key)


class S3Info(namedtuple('S3Conn', 'client bucket prefix')):
    def delete(self, key) -> str:
        return self.client.delete_object(Bucket=self.bucket, Key=key)

    def read(self, key) -> str:
        with NamedTemporaryFile() as tmp:
            self.client.download_fileobj(
                Bucket=self.bucket,
                Key=key,
                Fileobj=tmp
            )
            tmp.flush()
            with open(tmp.name, 'r', encoding='utf8') as tmp_reader:
                return tmp_reader.read()

    def write(self, key, string):
        with NamedTemporaryFile('w', encoding='utf8') as writer:
            writer.write(string)
            writer.flush()
            with open(writer.name, 'rb') as reader:
                self.client.upload_fileobj(Fileobj=reader, Bucket=self.bucket, Key=key)

    @staticmethod
    def parse_s3_url(s3url):
        parsed_url = urlparse(s3url)
        return parsed_url.netloc, parsed_url.path.lstrip('/')
