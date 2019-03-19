# -*- coding: UTF8 -*-
"""
Created on 2019-02-20

@author: liushengqiang
"""


import os
import time
import codecs
from common import config


def open(file_name, mode='a+', encoding='utf8', errors='strict', buffering=1):
    """
    创建一个临时文件，用file_name做基本名称，当前时间做后缀。并用可读写方式打开
    """
    file_obj = None
    if file_name is not None and len(file_name) > 0:
        config_obj = config.CConfigReader.instance()
        temp_folder = os.path.join(config_obj.getPara('cache_path'))
        try:
            os.stat(temp_folder)
        except:
            os.makedirs(temp_folder)
        file_name += '_' + time.strftime('%Y-%m-%d %H_%M_%S', time.localtime(time.time()))
        file_obj = codecs.open(os.path.join(temp_folder, file_name), mode, encoding, errors, buffering)

    return file_obj


def close(file_obj, flag=False):
    file_obj.close()
    if flag:
        try:
            os.remove(file_obj.name)
        except:
            pass
