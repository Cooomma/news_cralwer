# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class Hk01Item(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    artical_id = scrapy.Field()
    title = scrapy.Field()
    editor = scrapy.Field()
    release_ts = scrapy.Field()
    last_updated_ts = scrapy.Field()
    abstract = scrapy.Field()
    paragraph = scrapy.Field()
    tag_ids = scrapy.Field()
    tag_names = scrapy.Field()
    tags = scrapy.Field()
    spider_ts = scrapy.Field()
    source = scrapy.Field()
