# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import sys
from datetime import datetime
import json

import boto3
import redis
from HK01 import parser


class Hk01Pipeline(object):

    def __init__(self):
        # self.s3 = boto3.resource('s3')
        self.redis = redis.StrictRedis(host=os.environ['REDIS_HOST'], port=6379, db=0)
        self.s3_bucket = os.environ['S3_BUCKET']
        pass

    def open_spider(self, spider):
        spider.last_id = self.redis.get("HK01_LAST_CRAWL_ID")
        pass

    def process_item(self, item, spider):
        self._local_storage(item)
        return item

    def close_spider(self, spider):
        pass

    def _s3fs(self, item):
        self.redis.set("HK01_LAST_CRAWL_ID", int(item.get("article_id")))

        key = "HK01/dt={dt}/{article_id}.json".format_map(
            {'dt': datetime.strptime(item.get('release_ts'), '%Y-%m-%d %H:%M').strftime('%Y-%m-%d'),
             'article_id': item.get('article_id')})
        self.s3.Bucket(s3_bucket).put_object(
            ACL='bucket-owner-full-control',
            Body=json.dumps(item, ensure_ascii=False, sort_keys=True).encode(),
            Key=key,
            StorageClass='STANDARD'
        )

    def _local_storage(self, item):
        local_dir = "local_fs/HK01/dt={dt}/".format_map(
            {'dt': parser.ts_to_timestr(item.get('release_ts'))})
        if not os.path.isdir(local_dir):
            os.makedirs(local_dir, mode=0o777)
        local_path = local_dir + "{article_id}.json".format_map(
            {'dt': parser.ts_to_timestr(item.get('release_ts')),
             'article_id': item.get('article_id')})
        with open(local_path, 'w') as w:
            w.write(json.dumps(item, ensure_ascii=False, sort_keys=True))
