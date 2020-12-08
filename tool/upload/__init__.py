from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection, AsyncElasticsearch, AIOHttpConnection, TransportError
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


def connect(*, is_test=False):
    global es
    es = Elasticsearch(
        hosts=os.getenv('ES_HOSTS').split(',') or ['127.0.0.1'],
        port=os.getenv('ES_PORT'),
        http_auth=auth,
        use_ssl=not is_test,
        verify_certs=not is_test,
        connection_class=RequestsHttpConnection,
        timeout=int(config['REQUEST']['timeout']),
        max_retries=int(config['REQUEST']['max_retries']),
        retry_on_timeout=True,
        )


def bulk(index, /, posts_info=None, is_test=False):
    ok = False
    retry = False
    if not posts_info:
        return ok, retry

    if not es:
        connect(is_test=is_test)

    start = time.time()
    try:
        result = helpers.bulk(es, gendata(index, posts_info))
    except helpers.BulkIndexError as e:
        logger.error(f"Bulk 失敗", exc_info=True)
        logger.error(e)
    except TransportError as e:
        logger.error(f"Bulk 失敗, {e.error}: {e.status_code}")
        retry = True
    else:
        success_num, fail_info = result
        logger.info(f'上傳完 {success_num} 筆資料, 失敗 {len(fail_info)} 筆: 花費 {time.time() - start} 秒')
        if len(fail_info) > 0:
            logger.error(fail_info)
        else:
            ok = True
    # try:
    #     if bulk_file:
    #         es.bulk(bulk_file)
    # except TransportError as e:
    #     logger.error(f"Bulk 失敗, {e.error}: {e.status_code}, {json.dumps(e.info)}")
    #     retry = True
    #     return ok, retry

    return ok, retry


def gendata(index, /, results):

    for result in results:
        if not result:
            continue
        yield {
            "_index": index,
            "_id": str(result['id']),
            **result
        }

        # bulk_file += '{ "index" : { "_index" : "' + index + \
        #     '", "_type" : "_doc", "_id" : "' + str(result['id']) + '"} }\n'
        # bulk_file += json.dumps(result) + '\n'
        # count += 1
        # total += 1
        # if count == int(config['UPLOAD']['per_record']):
        #     es.bulk(bulk_file)
        #     count = 0
        #     bulk_file = ''
