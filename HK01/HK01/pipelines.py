# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import gzip
import json
import logging
import os

import boto3
import redis

from HK01 import database, parser
from HK01.database import HK01Progress

logger = logging.getLogger()


class Hk01Pipeline(object):

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.s3_bucket = os.environ['S3_BUCKET']
        self.redis = redis.StrictRedis(
                host=os.environ['REDIS_HOST'], 
                port=6379, 
                db=0)
        engine, meta = database.new_engine_and_metadata()
        self.table = HK01Progress(engine, meta)
        self.upload_todos = set()

    def open_spider(self, spider):
        pass

    def process_item(self, item, spider):
        self._local_storage(item)
        dt, idx = self._get_dt_and_id(item)
        if not self.is_crawled_article(dt, item):
            self._local_gzip(item)
            self._s3fs(item)
        return item

    def close_spider(self, spider):
        if len(self.upload_todos) > 0:
            self._upload_gz()

    def _s3fs(self, item):
        dt, article_id = self._get_dt_and_id(item)
        key = "HK01/dt={dt}/{article_id}.json".format_map(
            {'dt': dt, 'article_id': article_id})
        self.s3.Bucket(self.s3_bucket).put_object(
            ACL='bucket-owner-full-control',
            Body=json.dumps(item, ensure_ascii=False, sort_keys=True).encode(),
            Key=key,
            StorageClass='STANDARD'
        )
        logger.info("Upload {article_id} to s3://{bucket}/{key}".format_map(
            {'article_id': article_id, 'bucket': self.s3_bucket, 'key': key}))

    def _upload_gz(self):
        while len(self.upload_todos) > 0:
            logger.info("Upload todos: {}".format(self.upload_todos))
            dt = self.upload_todos.pop()
            key = "news/HK01/dt={}/article.gz".format(dt)
            local_path = "/data/news-etl/HK01/dt={}/articles.gz".format(dt)
            self.s3.meta.client.upload_file(local_path, 'comma-etl', key)
            logger.info("Upload {key} to s3://comma-etl/{key}".format_map({'key': key}))

    def _local_storage(self, item):
        dt, article_id = self._get_dt_and_id(item)
        local_dir = "/data/news-raw/HK01/dt={dt}/".format_map(
            {'dt': dt})
        os.makedirs(local_dir, mode=0o777, exist_ok=True)
        local_path = local_dir + "{article_id}.json".format_map(
            {'dt': dt, 'article_id': article_id})
        with open(local_path, 'w', encoding='utf-8') as w:
            w.write(json.dumps(item, ensure_ascii=False, sort_keys=True))
        self.redis.set("HK01_LAST_CRAWL_ID", int(article_id))
        self.table.insert_article_progress(article_id, local_path)

    def _local_gzip(self, item):
        dt, article_id = self._get_dt_and_id(item)
        local_dir = "/data/news-etl/HK01/dt={}/".format(dt)
        os.makedirs(local_dir, mode=0o777, exist_ok=True)
        gz_file = local_dir + 'articles.gz'
        gz = gzip.GzipFile(gz_file, mode='ab')
        gz.write(json.dumps(item, ensure_ascii=False, sort_keys=True).encode())
        gz.write(b'\n')
        gz.close()
        self.upload_todos.add(dt)
        logger.info('{article_id} is written in {gz_file}'.format_map({
            'article_id': article_id,
            'gz_file': gz_file
        }))

    @staticmethod
    def is_crawled_article(dt, article_id):
        # TODO: Use DB to save it instead of local fs cache
        raw_file = "/data/news-raw/HK01/dt={dt}/{article_id}.json".format_map(
            {'dt': dt, 'article_id': article_id})
        if os.path.isfile(raw_file):
            logger.info('{} is existed.'.format_map(article_id))
            return True
        else:
            return False

    @staticmethod
    def _get_dt_and_id(item):
        dt = parser.ts_to_timestr(item.get('release_ts'))
        idx = item.get('article_id')
        return dt, idx
