# -*- coding: UTF-8 -*-


from common import log
from data_parse.meteorology.base import CMeteorologicalBaseData
from data_parse.water_quality.base import CWaterQualityBaseData

if __name__ == "__main__":
    try:
        log.CLog.instance().init()
        log.CLog.instance().logger("DataImport").info("DataImport begin...")

        meteorological_obj = CMeteorologicalBaseData()
        meteorological_obj.do()
        water_quality_obj = CWaterQualityBaseData()
        water_quality_obj.do()

        log.CLog.instance().logger("DataImport").info("DataImport End!")
        log.CLog.instance().end()
    except Exception as e:
        log.CLog.instance().logger("DataImport").exception("%s..." % e)
        exit(1)
    finally:
        exit(0)
