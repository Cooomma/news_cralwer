from urllib.parse import unquote
from datetime import datetime

import os
import redis
import scrapy


ARTICAL_URL = 'https://www.hk01.com/article/{}'


class Hk01Spider(scrapy.Spider):
    name = 'HK01'

    @staticmethod
    def _get_artical_url(artical_id):
        return ARTICAL_URL.format(artical_id)

    @staticmethod
    def _clean_tag_ids(tag_ids):
        return [int(id.split('/')[-1])
                for id in tag_ids if 'javascript' not in id]

    @staticmethod
    def _clean_ts(ts):
        ts = ts.replace('\t', '').replace('\n', '')
        if '最後更新日期：' in ts:
            ts = ts.replace('最後更新日期：', '')
        if '發佈日期：' in ts:
            ts = ts.replace('發佈日期：', '')
        return datetime.strptime(ts, '%Y-%m-%d %H:%M')

    @staticmethod
    def _get_source(paragraph):
        src = paragraph[-2]
        if '（' in src or '）' in src:
            src = src[1:-1].split('、')
        else:
            src = ''
        return src

    @staticmethod
    def _zip_tags(tag_ids, tag_names):
        cleaned_tag_ids = [int(id.split('/')[-1])
                           for id in tag_ids if 'javascript' not in id]
        return dict(zip(cleaned_tag_ids, tag_names))

    def start_requests(self):
        # TODO: Get last crawler ID
        # r = redis.StrictRedis(host=os.environ['REDIS_HOST'], port=6379, db=0)
        # start_id = int(r.get('HK01_LAST_CRAWL_ID'))
        # end_id = start_id + 5
        start_id = 0
        end_id = 160000
        for artical_id in range(start_id, end_id):
            url = ARTICAL_URL.format(artical_id)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            channel = unquote(response.url.split('/')[3])
            artical_id = response.url.split('/')[4]
        except:
            channel = ''
            artical_id = 0

        try:
            title = response.css('div.article_tit h1::text').extract_first().replace(u'\u3000', u'')
        except:
            title = ""

        try:
            editors = []
            for editor in response.css('div.editor::text').extract():
                editors.append(editor.replace('撰文：', '').replace('\t', '').replace('\n', ''))
        except:
            editors = []

        try:
            ts = response.css('div.date::text').extract()
            release_ts = self._clean_ts(ts[0])
            last_updated_ts = self._clean_ts(ts[1])
        except:
            release_ts = None
            last_updated_ts = None

        try:
            abstract = response.css('li.article_summary_pt h2::text').extract_first().replace(u'\u3000', u'')
        except:
            abstract = ''

        try:
            paragraph = '\n'.join(response.css('p::text').extract())
        except:
            paragraph = []

        try:
            tag_ids = self._clean_tag_ids(response.css('div.tag_txt a[href]::attr(href)').extract())
        except:
            tag_ids = []

        try:
            tag_names = response.css('div.tag_txt h4::text').extract()
        except:
            tag_names = []

        try:
            tags = self._zip_tags(response.css('div.tag_txt a[href]::attr(href)').extract(
            ), response.css('div.tag_txt h4::text').extract())
        except:
            tags = []

        try:
            sources = self._get_source(response.selector.xpath(
                '/html/body/section/div[2]/div[1]/div[1]/section[2]').xpath('.//p/text()').extract())
        except:
            sources = []

        item = {
            'url': str(response.url),
            'article_id': int(artical_id),
            'title': str(title),
            'channel': str(channel),
            'editors': editors,
            'release_ts': str(release_ts.strftime('%Y-%m-%d %H:%M')),
            'last_updated_ts': str(last_updated_ts.strftime('%Y-%m-%d %H:%M')),
            'abstract': str(abstract),
            'paragraph': str(paragraph),
            'tag_ids': tag_ids,
            'tag_names': tag_names,
            'tags': tags,
            'spider_ts': int(datetime.now().timestamp() * 1000),
            'sources': sources
        }
        yield item
