# -*- coding: UTF8 -*-
"""
Created on 2019-02-20

@author: liushengqiang
"""


class CConfigReader(object):
    __instance = None

    @staticmethod
    def instance():
        if CConfigReader.__instance is None:
            CConfigReader.__instance = CConfigReader()

        return CConfigReader.__instance

    def __init__(self):
        self.lines = []
        self.load()

    def load(self, config_path='config.ini'):
        self.lines = self.readlines(config_path)

    def readlines(self, path):
        lines = []

        file_obj = open(path, 'r', encoding='utf8')
        try:
            lines = file_obj.readlines()
        except Exception as e:
            file_obj.close()
            print(e)
            raise
        finally:
            file_obj.close()

        return lines

    def getPara(self, name):
        path = ""
        for line in self.lines:
            if 0 == line.find(name + '='):
                path = line[len(name + '='):].strip()
                break

        return path
