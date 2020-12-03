from elasticsearch import Elasticsearch, RequestsHttpConnection
import boto3
import json
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


host = os.getenv('ES_HOST')
region = os.getenv('ES_REGION')  # e.g. us-west-1
service = 'es'
auth = None

credentials = boto3.Session().get_credentials()
if credentials:
    logger.info('By aws auth')
    from requests_aws4auth import AWS4Auth
    auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
else:
    logger.info('By basic auth')
    auth = (os.getenv('ES_USER'), os.getenv("ES_PASSWD"))
es = None
def connect():
    global es
    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)

def bulk(index, /, results):
    if not es:
        connect()
    bulk_file = ''
    for result in results:
        bulk_file += '{ "index" : { "_index" : "' + index + \
            '", "_type" : "_doc", "_id" : "' + str(result['id']) + '"} }\n'
        bulk_file += json.dumps(result) + '\n'

    es.bulk(bulk_file)
