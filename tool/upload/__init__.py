import asyncio
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import json
import os
# For example, my-test-domain.us-east-1.es.amazonaws.com
host = os.environ['HOST']
region = os.environ['REGION']  # e.g. us-west-1

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                   region, service, session_token=credentials.token)
es = None
def connect():
    global es
    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
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
