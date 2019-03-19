# -*- coding: UTF8 -*-
"""
Created on 2019-02-20

@author: liushengqiang
"""



import os
from common import config


class CSqlLoader(object):
    def __init__(self):
        self.sql_srcipts = []

    def init(self):
        path = config.CConfigReader.instance().getPara("sql_script")
        self.LoadSqlScriprts(path)

    def LoadSqlScriprts(self, path):
        for root, dirs, files in os.walk(path):
            for file in files:
                if file[-4:] == ".sql":
                    file_path = os.path.join(root, file)
                    lines = config.CConfigReader.instance().readlines(file_path)
                    self.sql_srcipts += lines

    def GetSqlScript(self, name):
        sqlcmd = ''
        found_flag = False
        function_flag = False

        for line in self.sql_srcipts:
            if not found_flag:
                lowline = line.lower()
                lowname = name.lower()

                pos = lowline.find(lowname)
                if pos < 0:
                    continue

                if lowline.find('create') == -1:
                    continue

                if lowline.find("(") != -1:
                    lowline = lowline[:line.find("(")]

                keyword = lowline.split()[-1]
                if keyword == lowname:
                    sqlcmd += line
                    found_flag = True
                    if lowline.find('function') >= 0:
                        function_flag = True

            else:
                sqlcmd += line
                if function_flag:
                    if line.find('$$;') >= 0 or line.find('$BODY$;') >= 0:  # '$$;' 鍑芥暟缁撴潫 鏍囧織
                        break
                else:
                    if line.rfind(';') >= 0:  # ';' sql璇悕鐨勭粨鏉� 鏍囧織
                        break

        if sqlcmd == '':
            return None

        return sqlcmd
