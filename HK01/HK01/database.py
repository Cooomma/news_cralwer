import logging
import os
import time
import configparser

import sqlalchemy
from sqlalchemy import INT, TIMESTAMP, Boolean, Column, String, Table, func
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import MetaData
from sqlalchemy.sql.expression import delete, insert, text, update

logger = logging.getLogger()
CONFIG_PATH = os.path.expanduser('~/.secret/credentials')


class BaseModel(object):
    def __init__(self, engine, metadata, table, role='reader'):
        self.engine = engine
        self.metadata = metadata
        self.table = table
        self.role = role

    def execute(self, stmt):
        return self.engine.execute(stmt)


def new_engine_and_metadata(db_conf=None):
    if os.path.isfile(CONFIG_PATH):
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH)
        db_conf = dict(config['db'])
    else:
        raise FileNotFoundError
    settings = {
        'max_overflow': -1,
        'pool_size': 8,
        'pool_recycle': 1024,
        'pool_timeout': 800,
    }
    db_connection_str = 'mysql://{username}:{password}@{host}:{port}/{db_name}'.format_map(db_conf)
    engine = sqlalchemy.create_engine(db_connection_str, **settings)
    metadata = MetaData(bind=engine)

    return engine, metadata


class HK01Progress(BaseModel):

    def __init__(self, engine, metadata, role='reader'):
        table = Table(
            'hk01_progress',
            metadata,
            Column('id', INT, primary_key=True, autoincrement=True),
            Column('article_id', INT),
            Column('crawl_ts', INT),
            Column('path', String),
            Column('updated_at', TIMESTAMP, default=func.now()),
        )
        super().__init__(engine, metadata, table, role)

    def get_article_path(self, article_id):
        stmt = text('SELECT crawl_ts, path FROM hk01_progress WHERE article_id = {}'.format(article_id))
        row = self.execute(stmt).fetchone()
        logger.info("DB Record of article_id = {article_id} : {row}".format_map({'article_id': article_id, 'row': row}))
        if row:
            return {'crawl_ts': row[0], 'path': row[1]}
        else:
            return None

    def insert_article_progress(self, article_id, path):
        row = {'article_id': int(article_id), 'crawl_ts': int(time.time()), 'path': str(path)}
        stmt = text(
            'INSERT INTO hk01_progress (article_id, crawl_ts, path) VALUES ({article_id}, {crawl_ts}, "{path}") ON DUPLICATE KEY UPDATE article_id = {article_id}'.format_map(row))
        self.execute(stmt)

    def get_all_article_ids(self):
        stmt = text('SELECT distinct(article_id) FROM hk01_progress ORDER BY article_id ASC')
        cursor = self.execute(stmt)
        row = cursor.fetchone()
        while row:
            yield row[0]
            row = cursor.fetchone()

    def get_last_crawled_article_id(self):
        stmt = text('SELECT distinct(article_id) FROM hk01_progress ORDER BY article_id DESC LIMIT 1')
        cursor = self.execute(stmt)
        return cursor.fetchone()[0]
