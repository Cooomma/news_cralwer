import logging
import os
import time

import sqlalchemy
from sqlalchemy import INT, TIMESTAMP, Boolean, Column, String, Table, func
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import MetaData
from sqlalchemy.sql.expression import delete, insert, text, update

logger = logging.getLogger()


class BaseModel(object):
    def __init__(self, engine, metadata, table, role='reader'):
        self.engine = engine
        self.metadata = metadata
        self.table = table
        self.role = role

    def execute(self, stmt):
        return self.engine.execute(stmt)


def new_engine_and_metadata(db_conf=None):
    settings = {
        'max_overflow': -1,
        'pool_size': 8,
        'pool_recycle': 1024,
        'pool_timeout': 800,
    }
    if db_conf is None:
        db_conf = {
            'host': os.environ["MYSQL_DATABASE_HOST"],
            'port': os.environ["MYSQL_DATABASE_PORT"],
            'username': os.environ["MYSQL_DATABASE_USERNAME"],
            'password': os.environ["MYSQL_DATABASE_PASSWORD"],
            'db_name': os.environ["MYSQL_DATABASE_NAME"],
        }

    db_connection_str = 'mysql://{}:{}@{}:{}/{}'.format(
        db_conf['username'],
        db_conf['password'],
        db_conf['host'],
        db_conf['port'],
        db_conf['db_name']
    )

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
