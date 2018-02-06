import os

from HK01 import database
from HK01.database import HK01Progress


class Validators(object):

    def __init__(self):
        engine, meta = database.new_engine_and_metadata()
        self.table = HK01Progress(engine, meta)

    def _is_article_existed_in_db(self):
        self._status = self.table.get_article_path(self.article_id)
        if self._status:
            return True
        else:
            return False

    def _is_article_existed_in_local_fs(self):
        if self._status:
            path = self._status.get('path')
            return bool(os.path.isfile(path))
        else:
            return False

    def check(self, article_id):
        self.article_id = article_id
        return bool(self._is_article_existed_in_db() & self._is_article_existed_in_local_fs())
