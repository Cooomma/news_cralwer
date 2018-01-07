from urllib.parse import unquote
from datetime import datetime
from dateutil import tz

import os
import time
import redis
import scrapy
from scrapy.loader import ItemLoader


from HK01 import parser
from HK01.items import Hk01Item

ARTICAL_URL = 'https://www.hk01.com/article/{}'


class Hk01Spider(scrapy.Spider):
    name = 'HK01'

    def start_requests(self):
        # TODO: Get last crawler ID
        r = redis.StrictRedis(host=os.environ['REDIS_HOST'], port=6379, db=0)
        start_id = int(r.get('HK01_LAST_CRAWL_ID'))
        end_id = start_id + 5
        for artical_id in range(start_id, end_id):
            url = ARTICAL_URL.format(artical_id)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        # Extract CSS
        channel = unquote(response.url.split('/')[3])
        article_id = response.url.split('/')[4]
        title = parser.extract_title(response.css('div.article_tit h1::text').extract_first())
        editors = parser.extract_editors(response.css('div.editor::text').extract())
        release_ts = parser.extract_release_ts(response.css('div.date::text').extract())
        last_updated_ts = parser.extract_last_update_ts(response.css('div.date::text').extract())
        abstract = parser.extract_abstract(response.css('li.article_summary_pt h2::text').extract_first())
        paragraph = parser.extract_paragraph(response.css('p::text').extract())
        tag_names = parser.extract_tag_names(response.css('div.tag_txt h4::text').extract())
        tag_ids = parser.extract_tag_ids(response.css('div.tag_txt a[href]::attr(href)').extract())
        tags = parser.zip_tags(tag_ids, tag_names)
        sources = parser.extract_sources(response.css('p::text').extract())

        # Add article item
        '''
        article = ItemLoader(item=Hk01Item(), response=response)

        article.add_value('article_id', artical_id)
        article.add_value('channel', channel)
        article.add_value('title', title)
        article.add_value('editor', editors)
        article.add_value('release_ts', release_ts)
        article.add_value('abstract', abstract)       
        article.add_value('paragraph', paragraph)
        article.add_value('tag_ids', tag_ids)
        article.add_value('tag_names', tag_names)
        article.add_value('tags', tags)
        article.add_value('spider_ts', int(time.time()))
        article.add_value('sources', sources)
        article.add_value('last_updated_ts', last_updated_ts)
        article.add_value('url', ARTICAL_URL.format(artical_id))
        '''

        item = {
            'article_id': article_id,
            'channel': channel,
            'title': title,
            'editors': editors,
            'release_ts': release_ts,
            'abstract': abstract,
            'paragraph': paragraph,
            'tag_ids': tag_ids,
            'tag_names': tag_names,
            'tags': tags,
            'spider_ts': int(time.time()),
            'sources': sources,
            'last_updated_ts': last_updated_ts,
            'url': ARTICAL_URL.format(article_id),
        }

        yield item
