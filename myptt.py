'''
0. settings.py
    - request
        - 逾期嘗試
        - 錯誤嘗試
        - 間隔
    - 目標網站domain
    - async數量
    - log
        - 儲存位置
        - 格式
        - 層級
1. Read list
    - 從檔案
    - 從server
2. 異步搜集
    - paramater
        - 一次可同時多少個
        - 蒐集格式
        - 權限
    - 錯誤處理
3. 處理蒐集資料
    - 欄位過濾
    - 轉換成指定格式
    - 儲存至
4. 拿取下個蒐集目標
'''
import os
import configparser
from dotenv import load_dotenv
load_dotenv()
config = configparser.ConfigParser()
config.read(os.getenv('SETTING'))
with open(os.getenv('SETTING'), 'w') as f:
    config.write(f)

import logging
import logging.config

logging.config.fileConfig(
    os.path.dirname(os.path.abspath(__file__))+'/'+os.getenv('LOG_SETTING'))
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
es_log = logging.getLogger("elasticsearch")
es_log.setLevel(logging.CRITICAL)

import sys
import traceback
import argparse
import httpx
import asyncio
import time
import re
import json
from bs4 import BeautifulSoup

from tool import web_parse, upload

COOKIES = {'over18': '1'}


async def main(url, *, from_page=0, to_page=1, max_page=-1, per_page=3, all_post=False, board_name=None, store_local=True, fetch_comment=False):
    if all_post:
        base_url = config['PTT_ALLPOST']['url']
    elif board_name:
        base_url = config['PTT_BOARD']['url'] + '/' + board_name

    if not from_page:
    # 拿取index.html的初始頁面和前一頁頁數
        with httpx.Client(cookies=COOKIES, timeout=int(config['REQUEST']['timeout'])) as client:
            oldest, prev, next_, latest = fetch_last_page(client, url)
            start = int(latest)
    else:
        start = int(from_page)

    end = int(to_page)
    # 最多蒐集幾頁
    if max_page > 0:
        end = start - max_page + 1
        if end < 1:
            end = 1
    # 產生要蒐集的頁數連結
    cur_page = start
    while cur_page >= end:
        links = []
        for page_num in range(cur_page, max(cur_page-per_page, end-1), -1):
            links.append((page_num, base_url+f'/index{page_num}.html'))
        logger.debug(links)
        async with httpx.AsyncClient(cookies=COOKIES, timeout=int(config['REQUEST']['timeout'])) as client:
            # 蒐集每一頁的文章連結
            tasks = [asyncio.create_task(fetch_post_list(client, page_num, link)) for page_num, link in links]
            try:
                result = await asyncio.gather(*tasks, return_exceptions=True)
            except asyncio.TimeoutError:
                logger.error(traceback.format_exception(*sys.exc_info()))
            except Exception:
                logger.error(traceback.format_exception(*sys.exc_info()))
            else:
                post_links = []  # [link]
                web_parse.parse_post_links(result, post_links=post_links, all_post=all_post)
                logger.debug(f'共有 {len(post_links)} 篇文章要蒐集')

            # 蒐集現有文章連結內容
            tasks = [asyncio.create_task(fetch_post_content(
                client, link)) for link in post_links]
            try:
                result = await asyncio.gather(*tasks)
            except asyncio.TimeoutError:
                logger.error(traceback.format_exception(*sys.exc_info()))
            except Exception:
                logger.error(traceback.format_exception(*sys.exc_info()))
            else:
                posts_info = []  # 蒐集到的文章資訊
                web_parse.parse_posts(result, posts_info=posts_info, fetch_comment=fetch_comment)
                if store_local:
                    await asyncio.to_thread(record, 'result.rec', posts_info)
                else:
                    upload.bulk(os.getenv('ES_INDEX'), posts_info)

            cur_page -= per_page

def fetch_last_page(client, /, url):
    try:
        response = client.get(url)
        response.raise_for_status()
    except httpx.RequestError as exc:
        etype, value, tb = sys.exc_info()
        logger.error(f'蒐集最新 {exc.request.url} 文章列表失敗: {etype}')
    except httpx.HTTPStatusError as exc:
        logger.error(f'蒐集最新 {exc.request.url} 文章列表失敗: {exc.response.status_code}')
    else:
        soup = BeautifulSoup(response.text, 'html.parser')
        page_nums = web_parse.parse_page_num(soup)

    return page_nums


async def fetch_post_list(client, /, page_num, url):
    try:
        response = await client.get(url)
        response.raise_for_status()
    except httpx.RequestError as exc:
        etype, value, tb = sys.exc_info()
        logger.error(f'蒐集 {exc.request.url} 文章列表失敗: {etype}')
    except httpx.HTTPStatusError as exc:
        logger.error(f'蒐集 {exc.request.url} 文章列表失敗: {exc.response.status_code}')
    else:
        return response

async def fetch_post_content(client, /, url):
    try:
        response = await client.get(url)
        response.raise_for_status()
    except httpx.RequestError as exc:
        etype, value, tb = sys.exc_info()
        logger.error(f'蒐集 {exc.request.url} 文章失敗: {etype}')
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 404:
            logger.debug(f'蒐集 {exc.request.url} 文章失敗: {exc.response.status_code}')
    else:
        return response

def record(file, r):
    with open(file, 'a') as f:
        json.dump(r, f, ensure_ascii=False)


def read_list(file):
    with open(file, 'r') as f:
        pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--list', help="蒐集清單檔案", default=None, metavar='檔名')
    parser.add_argument('-a', '--allpost', help="從allpost板中蒐集文章", action='store_true')
    parser.add_argument('--hotboard', help="蒐集熱門清單", action='store_true')
    parser.add_argument('-b', '--board', help="蒐集指定板，板名不分大小寫", default=None, metavar='板名')
    parser.add_argument('-u', '--url', help="要爬取的網頁url", default=None, metavar='網址')
    parser.add_argument('--max', help="最多爬取幾頁，-1則為爬到第1頁", default=-1, metavar='頁數', type=int)
    parser.add_argument('--per', help="一次同時爬取幾頁", default=3, metavar='頁數', type=int)
    parser.add_argument('--test', help="僅將結果存到local file", action='store_true')
    parser.add_argument('--loop', help="是否循環蒐集", action='store_true')
    parser.add_argument('--comment', help="是否蒐集回覆", action='store_true')
    args = parser.parse_args()

    max_page = args.max
    per_page = args.per
    main_url = None
    if args.list:
        read_list(args.list)
        sys.exit(0)
    elif args.allpost:
        main_url = config['PTT_ALLPOST']['url']+'/index.html'
    elif args.board:
        main_url = config['PTT_BOARD']['url']+'/'+args.board+'/index.html'
    elif args.hotboard:
        main_url = config['PTT_HOTBOARD']['url']+'/hotboards.html'
        sys.exit(0)
    elif args.url:
        main_url = args.url
    else:
       parser.print_help()
       sys.exit(0)

    count = 0
    while args.loop:
        count += 1
        start = time.time()
        asyncio.run(main(main_url, all_post=args.allpost, board_name=args.board, max_page=max_page, per_page=per_page, store_local=args.test, fetch_comment=args.comment))
        end = time.time()
        logger.info(f'第 {count} 次循環蒐集結束: 花費 {end - start} 秒')

    if not args.loop:
        asyncio.run(main(main_url, all_post=args.allpost, board_name=args.board, max_page=max_page, per_page=per_page, store_local=args.test))


