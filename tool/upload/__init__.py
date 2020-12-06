from elasticsearch import Elasticsearch, RequestsHttpConnection, AsyncElasticsearch, AIOHttpConnection, Transport
import boto3
import json
import os, time
import logging
import configparser

config = configparser.ConfigParser()
config.read(os.environ.get('SETTING', 'settings.ini'))

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


region = os.getenv('ES_REGION')  # e.g. us-west-1
service = 'es'
auth = None
es = None

if os.environ.get('AUTH', 'basic') == 'aws':
    credentials = boto3.Session().get_credentials()
    logger.info('Operate by aws auth')
    from requests_aws4auth import AWS4Auth
    auth = AWS4Auth(credentials.access_key, credentials.secret_key,
                    region, service, session_token=credentials.token)
else:
    logger.info('Operate by basic auth')
    auth = (os.getenv('ES_USER'), os.getenv("ES_PASSWD"))


def connect():
    global es
    es = Elasticsearch(
        hosts=os.getenv('ES_HOSTS').split(',') or ['127.0.0.1'],
        port=os.getenv('ES_PORT'),
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=int(config['REQUEST']['timeout']),
        max_retries=int(config['REQUEST']['max_retries']),
        retry_on_timeout=True
        )

def bulk(index, /, results):
    if not es:
        connect()
    bulk_file = ''
    count = 0
    total = 0
    start = time.time()
    for result in results:
        if not result:
            continue

        bulk_file += '{ "index" : { "_index" : "' + index + '", "_type" : "_doc", "_id" : "' + str(result['id']) + '"} }\n'
        bulk_file += json.dumps(result) + '\n'
        count += 1
        total += 1
        if count == int(config['UPLOAD']['per_record']):
            es.bulk(bulk_file)
            count = 0
            bulk_file = ''

    if bulk_file:
        es.bulk(bulk_file)
    logger.info(f'上傳完 {total} 筆資料: 花費 {time.time() - start} 秒')
