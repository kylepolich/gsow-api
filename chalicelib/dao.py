from chalicelib.dynamo import DynamoDocstore, DynamoStream
import logging
import os


log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


def get_value_or_none(d, value, default=None):
    # We want to minimize the amount of sensitive data in .github/workflows/pythonapp.yml
    if value in d:
        return d[value]
    else:
        log.warning(f"Missing environment variable: {value}")
        return default


class DataAccessObject(object):


    def __init__(self, access_key, secret_key, region_name, table_name, stream_table):

        self.access_key = access_key
        self.secret_key = secret_key
        self.region_name = region_name
        self.docstore = DynamoDocstore(table_name, self.access_key, self.secret_key)
        self.streams = DynamoStream(stream_table, self.access_key, self.secret_key)


    def get_docstore(self):
        return self.docstore


    def get_streams(self):
        return self.streams

