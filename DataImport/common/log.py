# -*- coding: UTF8 -*-
"""
Created on 2019-02-20

@author: liushengqiang
"""



import os
import time
import sys
import logging


class CLog:
    __instance = None

    @staticmethod
    def instance():
        if CLog.__instance is None:
            CLog.__instance = CLog()
        return CLog.__instance

    def __init__(self):
        self.dictLogger = {}

        log_folder = os.path.join(os.getcwd(), 'log')
        try:
            os.stat(log_folder)
        except:
            os.makedirs(log_folder)

        log_file_name = 'log' + time.strftime('%Y-%m-%d %H_%M_%S', time.localtime(time.time())) + '.txt'
        self.strLog = os.path.join(log_folder, log_file_name)

    def init(self):
        # handle
        self.objSysHandler = CCommonSysoutHandler()
        self.objLogHandler = CCommonFileHandler(self.strLog)

        # formatter
        self.objFormatter = logging.Formatter("%(asctime)s - %(levelname)s: %(name)s----%(message)s")
        self.objSysHandler.setFormatter(self.objFormatter)
        self.objLogHandler.setFormatter(self.objFormatter)

    def logger(self, name):
        if name not in self.dictLogger.keys():
            objLogger = CCommonLogger(name)
            objLogger.addHandler(self.objSysHandler)
            objLogger.addHandler(self.objLogHandler)
            self.dictLogger[name] = objLogger
        return self.dictLogger[name]

    def end(self):
        for logger in self.dictLogger.values():
            logger.removeHandler(self.objLogHandler)
            logger.removeHandler(self.objSysHandler)
        self.objLogHandler.close()


class CCommonSysoutHandler(logging.StreamHandler):
    def __init__(self):
        logging.StreamHandler.__init__(self, sys.stdout)


class CCommonFileHandler(logging.FileHandler):
    def __init__(self, filename):
        logging.FileHandler.__init__(self, filename)


class CCommonLogger(logging.Logger):
    def __init__(self, name):
        logging.Logger.__init__(self, name)
