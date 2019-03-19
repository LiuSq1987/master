# -*- coding: UTF8 -*-
"""
Created on 2019-03-04

@author: liushengqiang
"""


from common import log
from src import check


if __name__ == "__main__":
    try:
        log.CLog.instance().init()
        log.CLog.instance().logger("MeteoricDataFileCheck").info("Check begin...")
        check.CCheck.instance().run()
        log.CLog.instance().logger("MeteoricDataFileCheck").info("Check end!")
        log.CLog.instance().end()
    except:
        log.CLog.instance().logger("MeteoricDataFileCheck").exception("error happend...")
        exit(1)
    finally:
        exit(0)
