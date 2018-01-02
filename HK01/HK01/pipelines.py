# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
from datetime import datetime
import json

import boto3
import redis


class Hk01Pipeline(object):

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.redis = redis.StrictRedis(host=os.environ['REDIS_HOST'], port=6379, db=0)

    def open_spider(self, spider):
        spider.last_id = self.redis.get("HK01_LAST_ID")

    def process_item(self, item, spider):
        self.redis.set("HK01_LAST_CRAWL_ID", int(item.get("article_id")))
        key = "HK01/dt={dt}/{article_id}.json".format_map(
            {'dt': datetime.strptime(item.get('release_ts'), '%Y-%m-%d %H:%M').strftime('%Y-%m-%d'),
             'article_id': item.get('article_id')})
        self.s3.Bucket("comma-fs").put_object(
            ACL='bucket-owner-full-control',
            Body=json.dumps(item, ensure_ascii=False).encode(),
            Key=key,
            StorageClass='STANDARD'
        )
        return item

    def close_spider(self, spider):
        pass
