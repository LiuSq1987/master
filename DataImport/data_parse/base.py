# -*- coding: UTF8 -*-
"""
Created on 2019-03-18

@author: liushengqiang
"""


from common import config
from common import database


class CBase(object):
    def __init__(self):
        self.config = config.CConfigReader.instance()
        # 数据库
        self.pg = database.pg_client()

    def init(self):
        self.pg.connect()

    def close(self):
        self.pg.close()
