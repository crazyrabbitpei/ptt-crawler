import logging
import os
import re
from datetime import datetime, timedelta
import pytz
import bs4
from bs4 import BeautifulSoup
from collections import defaultdict

import configparser
config = configparser.ConfigParser()
config.read(os.getenv('SETTING'))
tw_tz = pytz.timezone('Asia/Taipei')

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

def parse_page_num(soup):
    # 0: 最舊, 1: 上頁, 2: 下頁, 3: 最新
    pages = []
    for paging_bar in soup.find_all('div', class_='btn-group-paging'):
        paging = paging_bar.find_all('a')
        hrefs = [href.get('href') if href.get('href')
                 else None for href in paging]
        for href in hrefs:
            if not href:
                pages.append(None)
                continue
            page_pattern = re.search(r'index([\d]+).html', href)
            if not page_pattern:
                pages.append(None)
                continue
            pages.append(page_pattern.groups()[0])
    if not pages[3]:
        pages[3] = str(int(pages[1])+1)
    return pages

def parse_post_links(responses, *, post_links: list[str], all_post=False):

    for response in responses:
        if not response:
            continue
        soup = BeautifulSoup(response.text, 'html.parser')
        for title_div in soup.find_all('div', class_='title'):
            if not title_div.find('a'):
                continue
            title = title_div.find('a').get_text(strip=True)
            post_link = title_div.find('a').get('href')

            if all_post:
                board_block = re.search(r'\(([\w\-]+)\)$', title)
                if board_block:
                    board = board_block.groups()[0]
                    post_link = config['PTT']['url'] + post_link.replace('ALLPOST', board)
                    post_links.append(post_link)
            else:
                post_link = config['PTT']['url'] + post_link
                post_links.append(post_link)

    #logger.debug(responses)
    #logger.debug(post_links)

def parse_posts(responses, *, posts_info: list, fetch_comment=False):
    for response in responses:
        post_info = defaultdict(list)
        if not response:
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        #logger.debug(response.url)
        # 文章上方資訊
        for meta in soup.find_all('div', class_='article-metaline'):
            author_meta = re.search(r'作者\s*([^\s]+)', meta.get_text(strip=True))
            title_meta = re.search(r'標題\s*((?:Re|RE|re)\:)?\s*(\[[\w\-]+\])?\s*(.*)', meta.get_text(strip=True))
            time_meta = re.search(r'時間\s*([a-zA-Z\d: ]+)$', meta.get_text(strip=True))
            if author_meta:
                post_info['author'] = author_meta.groups()[0]
            elif title_meta:
                post_info['is_reply'] = title_meta.groups()[0] != None
                post_info['category'] = title_meta.groups()[1]
                post_info['title'] = title_meta.groups()[2].strip()
            elif time_meta:
                time = time_meta.groups()[0].strip()
                try:
                    parse_time = datetime.strptime(time, '%a %b %d %H:%M:%S %Y')
                    parse_time = parse_time.astimezone(tw_tz) - timedelta(hours=8)
                except:
                    # 不匯入錯誤日期的資料
                    logger.error(f'{response.url} 無法拿取正確發布日期: {time}')
                    continue
                    #post_info['time_error'] = True
                else:
                    time = parse_time.isoformat()
                    #post_info['time_error'] = False

                post_info['time'] = time
        # 原文和回覆區的分界線
        content_ip_bottom = None
        # f2為class都為「※」相關資訊，例如:發信站、文章網址、引述、編輯
        f2 = None
        f2_meta = soup.select('div#main-content > span.f2') or []
        # 找原po ip訊息
        for f2 in f2_meta:
            ip_meta = re.search(r'([\d]+\.[\d]+\.[\d]+\.[\d]+)', f2.get_text(strip=True))
            if ip_meta:
                post_info['ip'].append(ip_meta.groups()[0])
                # 若發信站後接的是文章網址訊息，則視為該f2 element是原文和回覆區的分界線
                if f2.next_sibling and f2.find_next_siblings('a') and f2.find_next_siblings('a')[0].get('href') == response.url:
                    content_ip_bottom = f2

        # 原文和回覆區的分界線被原po砍掉了，則採用最後編輯以上為原文
        if not content_ip_bottom:
            content_ip_bottom = f2

        post_info['content'] = ''
        # 有可以分割主文和回覆的物件
        if content_ip_bottom:
            # 拿取回覆區塊以上和以下的原文訊息，不會有引述和其它轉發的回覆在內，也不會有a tag裡的連結
            content = get_post_main(content_ip_bottom)
            post_info['content'] = content

            # 回覆
            if fetch_comment:
                post_info.update(get_commet_info(soup))
        # 略過無法區別回覆和主文的文章
        else:
            continue

        post_info['url'] = str(response.url)
        post_info['id'] = re.search(r'www\.ptt\.cc/bbs/(.+).html', post_info['url']).groups()[0]
        post_info['board'] = re.search(r'www\.ptt\.cc/bbs/(.+)/.+', post_info['url']).groups()[0]

        now = datetime.now()
        tw_now = now.astimezone(tw_tz)
        post_info['fetch_time'] = tw_now.isoformat()
        posts_info.append(post_info)


def get_post_main(content_ip_bottom):
    content = []
    p = content_ip_bottom.previous_sibling
    while p:
        if type(p) == bs4.element.NavigableString and p.strip():
            content.append(re.sub(r'\s+', ' ', p.strip()))
        p = p.previous_sibling
    p = content_ip_bottom.next_sibling
    # 以上原文是從下到上蒐集，所以必須倒過來才是原文順序
    content.reverse()
    # 以下原文是從上到下蒐集，所以append在後即可
    while p:
        if type(p) == bs4.element.NavigableString and p.strip():
            content.append(re.sub(r'\s+', ' ', p.strip()))
        p = p.next_sibling

    return ' '.join(content)


def get_commet_info(soup):
    post_info = {}
    up, down, normal = 0, 0, 0
    for element in soup.select('span.push-tag'):
        if element.get_text(strip=True) == '推':
            up += 1
        elif element.get_text(strip=True) == '噓':
            down += 1
        else:
            normal += 1
    post_info['push_tags'] = {'up': up, 'down': down, 'normal': normal}

    post_info['push_userids'] = [element.get_text(
        strip=True) for element in soup.select('span.push-userid')]
    post_info['push_contents'] = [re.sub(r'^: ?', '', re.sub(
        r'\s+', ' ', element.get_text(strip=True))) for element in soup.select('span.push-content')]
    post_info['push_ipdatetimes'] = [element.get_text(
        strip=True).strip() for element in soup.select('span.push-ipdatetime')]

    return post_info
