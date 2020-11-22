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
config = configparser.ConfigParser()
config.read(os.environ['SETTING'])
config['LOG']['path'] = os.environ.get('LOG_PATH', None) or os.path.dirname(os.path.abspath(__file__))+'/logs'
os.environ['LOG_PATH'] = config['LOG']['path']
with open(os.environ['SETTING'], 'w') as f:
    config.write(f)

import logging
import logging.config

logging.config.fileConfig(
    os.path.dirname(os.path.abspath(__file__))+'/'+config['LOG']['config'])
logger = logging.getLogger(__name__)

import sys
import traceback
import argparse
import httpx
import asyncio
import time
import re
import json
from bs4 import BeautifulSoup


from tool import web_parse


COOKIES = {'over18': '1'}


async def main(url, *, from_page=0, to_page=1, all_post=False, board_name=None):
    posts_info = [] # 蒐集到的文章資訊
    if all_post:
        per_page = int(config['PTT_ALLPOST']['per_page']) # 一次蒐集幾頁的文章連結，一頁最多20篇文章連結
        base_url = config['PTT_ALLPOST']['url']
        max_page = int(config['PTT_ALLPOST']['max_page'])
    elif board_name:
        per_page = int(config['PTT_BOARD']['per_page']) # 一次蒐集幾頁的文章連結，一頁最多20篇文章連結
        base_url = config['PTT_BOARD']['url'] + '/' + board_name
        max_page = int(config['PTT_BOARD']['max_page'])

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
        post_links = []  # [link]
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
                web_parse.parse_posts(result, posts_info=posts_info)
                await asyncio.to_thread(record, 'result.rec', posts_info)

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
        logger.error(f'蒐集 {exc.request.url} 文章失敗: {exc.response.status_code}')
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
    parser.add_argument('-l', '--list', help="蒐集清單檔案", default=None)
    parser.add_argument('-a', '--allpost', help="從allpost板中蒐集文章", action='store_true') # TODO: 測試版先測allpost，所以不指定參數時預設為開啟，正式版要改回store_true
    parser.add_argument('--hotboard', help="蒐集熱門清單", action='store_true')
    parser.add_argument('-b', '--board', help="蒐集指定板，板名不分大小寫", default=None)
    parser.add_argument('-u', '--url', help="要爬取的網頁url", default=None)
    args = parser.parse_args()

    main_url = None
    if args.list:
        read_list(args.list)
        sys.exit(0)
    elif args.allpost:
        main_url = config['PTT_ALLPOST']['url']+'/index.html'
    elif args.board:
        main_url = config['PTT_BOARD']['url']+args.board+'/index.html'
    elif args.hotboard:
        main_url = config['PTT_HOTBOARD']['url']+'/hotboards.html'
        sys.exit(0)
    elif args.url:
        main_url = args.url
    else:
       parser.print_help()
       sys.exit(0)

    start = time.time()
    asyncio.run(main(main_url, all_post=args.allpost, board_name=args.board))
    end = time.time()
    print(f'start: {start}, end: {end}, total: {end-start}')


