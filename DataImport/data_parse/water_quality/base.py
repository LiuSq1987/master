# -*- coding: UTF8 -*-
"""
Created on 2019-03-11

@author: liushengqiang
"""


import json
import datetime
import requests
# from decimal import *
from common import log
from common import config
from common import database
from common import common


class CWaterQualityBaseData(object):
    def __init__(self):
        self.log = log.CLog.instance().logger('Water Quality Base Data')
        self.config = config.CConfigReader.instance()

        # 数据库
        self.pg = database.pg_client()

        #
        self.factor_code = {}

    def do(self):
        try:
            self.pg.connect()
            if not self.pg.IsExistTable('water_quality_data'):
                self.pg.CreateTable_ByName('water_quality_data')
                self.pg.CreateIndex_ByName('idx_water_quality_data_station_no_record_time')

            if not self.pg.IsExistTable('water_station'):
                self.pg.CreateTable_ByName('water_station')
                self.pg.CreateIndex_ByName('idx_unq_water_station_station_no')

            self.load_water_station()
            self.load_water_quality_information()
            self.update_related_tbl()
        except Exception as e:
            print(e)
            raise
        finally:
            self.pg.close()

    def get_json_data_from_txt(self, path):
        try:
            lines = self.config.readlines(path)
            json_flag = False
            for line in lines:
                if json_flag:
                    return line[:-1]

                if line.find("[BEGIN DATA]") > 0:
                    json_flag = True

            return None
        except Exception as e:
            print(e)
            raise

    def get_json_data_from_html(self, path):
        try:
            response = requests.get(url=path)
            if response.status_code == requests.codes.ok:
                return response.text

            return None
        except Exception as e:
            print(e)
            raise

    def load_water_station(self):
        self.log.info('load water station...')
        try:
            # json_data = self.get_json_data_from_txt(r'./docs/water_station.out')
            json_data = self.get_json_data_from_html(r'http://10.8.101.10:20305/api/v0.1/water_station')
            multi_dicts = []
            if json_data is not None:
                multi_dicts = json.loads(json_data)

            self.pg.CreateTable_ByName('tmp_water_station')
            file_obj = common.open("tmp_water_station")
            for dict_info in multi_dicts:
                station_no = str('') if dict_info['Station_Code'] is None else str(dict_info['Station_Code'])
                station_name = str('') if dict_info['Station_Name'] is None else str(dict_info['Station_Name'])
                drainage_code = str('') if dict_info['Drainage_code'] is None else str(dict_info['Drainage_code'])
                area_code = str('') if dict_info['Area_Code'] is None else str(dict_info['Area_Code'])[:6]
                address = str('') if dict_info['Address'] is None else str(dict_info['Address'])
                station_type = str('') if dict_info['Station_Type'] is None else str(dict_info['Station_Type'])
                monitor_level = str('') if dict_info['Monitor_Level'] is None else str(dict_info['Monitor_Level'])
                longitude = str('') if dict_info['longitude'] is None else str(dict_info['longitude'])
                latitude = str('') if dict_info['latitude'] is None else str(dict_info['latitude'])
                file_obj.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %
                               (station_no, station_name, drainage_code, area_code, address,
                                station_type, monitor_level, longitude, latitude))

            file_obj.seek(0)
            self.pg.copy_from(file_obj, 'tmp_water_station')
            self.pg.commit()
            self.pg.CreateIndex_ByName('idx_tmp_water_station_station_no')
        except Exception as e:
            print(e)
            raise
        finally:
            file_obj.close()

    def load_water_quality_information(self):
        self.log.info('load water quality information...')
        try:
            # json_data = self.get_json_data_from_txt(r'./docs/water_quality_data.out')
            json_data = self.get_json_data_from_html(r'http://10.8.101.10:20305/api/v0.1/water_quality')
            multi_dicts = []
            if json_data is not None:
                multi_dicts = json.loads(json_data)

            self.pg.CreateTable_ByName('tmp_water_quality_data')
            file_obj = common.open("tmp_water_quality_data")
            for dict_info in multi_dicts:
                station_code = dict_info['Station_Code']
                station_name = dict_info['Station_Name']
                record_time = str('')
                if dict_info['RECORDTIME'] is not None and dict_info['RECORDTIME'] != '':
                    time_stamp = datetime.datetime.strptime(dict_info['RECORDTIME'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    record_time = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                f01 = str('') if dict_info['F01'] is None else str(dict_info['F01'])  # 温度
                f02 = str('') if dict_info['F02'] is None else str(dict_info['F02'])  # 湿度
                f03 = str('') if dict_info['F03'] is None else str(dict_info['F03'])  # 电压
                f04 = str('') if dict_info['F04'] is None else str(dict_info['F04'])  # 水压1(进样压1)
                f05 = str('') if dict_info['F05'] is None else str(dict_info['F05'])  # 水压2(源水压1)
                f06 = str('') if dict_info['F06'] is None else str(dict_info['F06'])  # 水压3(进样压2)
                f07 = str('') if dict_info['F07'] is None else str(dict_info['F07'])  # 气压
                f08 = str('') if dict_info['F08'] is None else str(dict_info['F08'])  # 水压4(源水压2)
                f09 = str('') if dict_info['F11'] is None else str(dict_info['F11'])  # PH值
                f10 = str('') if dict_info['F12'] is None else str(dict_info['F12'])  # 水温
                f11 = str('') if dict_info['F13'] is None else str(dict_info['F13'])  # 溶解氧
                f12 = str('') if dict_info['F14'] is None else str(dict_info['F14'])  # 浊度
                f13 = str('') if dict_info['F15'] is None else str(dict_info['F15'])  # 电导率
                f14 = str('') if dict_info['F16'] is None else str(dict_info['F16'])  # 高锰酸盐指数
                f15 = str('') if dict_info['F17'] is None else str(dict_info['F17'])  # 氨氮
                f16 = str('') if dict_info['F18'] is None else str(dict_info['F18'])  # 总磷
                f17 = str('') if dict_info['F19'] is None else str(dict_info['F19'])  # 总氮
                f18 = str('') if dict_info['F20'] is None else str(dict_info['F20'])  # TOC
                f19 = str('') if dict_info['F21'] is None else str(dict_info['F21'])  # 总酚（挥发酚）
                f20 = str('') if dict_info['F22'] is None else str(dict_info['F22'])  # 粪大肠菌群
                f21 = str('') if dict_info['F23'] is None else str(dict_info['F23'])  # 叶绿素
                f22 = str('') if dict_info['F24'] is None else str(dict_info['F24'])  # 水位
                f23 = str('') if dict_info['F25'] is None else str(dict_info['F25'])  # 流向
                f24 = str('') if dict_info['F26'] is None else str(dict_info['F26'])  # 流速（OPC_Flux）
                f25 = str('') if dict_info['F27'] is None else str(dict_info['F27'])  # 流量
                f26 = str('') if dict_info['F28'] is None else str(dict_info['F28'])  # 总氯
                f27 = str('') if dict_info['F29'] is None else str(dict_info['F29'])  # 化学需氧量
                f28 = str('') if dict_info['F30'] is None else str(dict_info['F30'])  # 五日生化需氧量
                f29 = str('') if dict_info['F31'] is None else str(dict_info['F31'])  # 石油类
                f30 = str('') if dict_info['F32'] is None else str(dict_info['F32'])  # 色度
                f31 = str('') if dict_info['F33'] is None else str(dict_info['F33'])  # 悬浮物
                f32 = str('') if dict_info['F34'] is None else str(dict_info['F34'])  # 有机氮
                f33 = str('') if dict_info['F35'] is None else str(dict_info['F35'])  # 蓝绿藻（藻蓝蛋白）
                f34 = str('') if dict_info['F36'] is None else str(dict_info['F36'])  # 氟化物
                f35 = str('') if dict_info['F37'] is None else str(dict_info['F37'])  # 氯化物
                f36 = str('') if dict_info['F38'] is None else str(dict_info['F38'])  # 氰化物
                f37 = str('') if dict_info['F39'] is None else str(dict_info['F39'])  # 汞
                f38 = str('') if dict_info['F40'] is None else str(dict_info['F40'])  # 铜
                f39 = str('') if dict_info['F41'] is None else str(dict_info['F41'])  # 铅
                f40 = str('') if dict_info['F42'] is None else str(dict_info['F42'])  # 铁
                f41 = str('') if dict_info['F43'] is None else str(dict_info['F43'])  # 锌
                f42 = str('') if dict_info['F44'] is None else str(dict_info['F44'])  # 锰
                f43 = str('') if dict_info['F45'] is None else str(dict_info['F45'])  # 铬
                f44 = str('') if dict_info['F46'] is None else str(dict_info['F46'])  # 镉
                f45 = str('') if dict_info['F47'] is None else str(dict_info['F47'])  # 砷
                f46 = str('') if dict_info['F48'] is None else str(dict_info['F48'])  # 硒
                f47 = str('') if dict_info['F49'] is None else str(dict_info['F49'])  # 生物毒性1（发光菌）
                f48 = str('') if dict_info['F50'] is None else str(dict_info['F50'])  # 生物毒性2（鱼法）
                f49 = str('') if dict_info['F51'] is None else str(dict_info['F51'])  # 硫化物
                f50 = str('') if dict_info['F52'] is None else str(dict_info['F52'])  # 硝态氮
                f51 = str('') if dict_info['F53'] is None else str(dict_info['F53'])  # 黄色物质
                f52 = str('') if dict_info['F54'] is None else str(dict_info['F54'])  # 三氮甲烷
                f53 = str('') if dict_info['F55'] is None else str(dict_info['F55'])  # 三氯乙烯
                f54 = str('') if dict_info['F56'] is None else str(dict_info['F56'])  # 四氮乙烯
                f55 = str('') if dict_info['F57'] is None else str(dict_info['F57'])  # 二氯甲烷
                f56 = str('') if dict_info['F58'] is None else str(dict_info['F58'])  # 1，2-二氯乙烷
                f57 = str('') if dict_info['F59'] is None else str(dict_info['F59'])  # 苯
                f58 = str('') if dict_info['F60'] is None else str(dict_info['F60'])  # 甲苯
                f59 = str('') if dict_info['F61'] is None else str(dict_info['F61'])  # 乙苯
                f60 = str('') if dict_info['F62'] is None else str(dict_info['F62'])  # 二甲苯
                f61 = str('') if dict_info['F63'] is None else str(dict_info['F63'])  # 氯苯
                f62 = str('') if dict_info['F64'] is None else str(dict_info['F64'])  # 1，2-二氯苯
                f63 = str('') if dict_info['F65'] is None else str(dict_info['F65'])  # 1，4-二氯苯
                f64 = str('') if dict_info['F66'] is None else str(dict_info['F66'])  # 异丙苯
                f65 = str('') if dict_info['F67'] is None else str(dict_info['F67'])  # 苯乙烯
                f66 = str('') if dict_info['F68'] is None else str(dict_info['F68'])  # 对、间二四苯
                f67 = str('') if dict_info['F69'] is None else str(dict_info['F69'])  # 1，2-二氯丙烷
                f68 = str('') if dict_info['F70'] is None else str(dict_info['F70'])  # 反式-1，2-二氯乙烯
                f69 = str('') if dict_info['F71'] is None else str(dict_info['F71'])  # 顺式-1，2-二氯乙烯
                f70 = str('') if dict_info['F72'] is None else str(dict_info['F72'])  # 镍
                f71 = str('') if dict_info['F73'] is None else str(dict_info['F73'])  # 绿藻
                f72 = str('') if dict_info['F74'] is None else str(dict_info['F74'])  # 硅甲藻
                f73 = str('') if dict_info['F75'] is None else str(dict_info['F75'])  # 隐藻
                f74 = str('') if dict_info['F76'] is None else str(dict_info['F76'])  # 经度(GPS经度)
                f75 = str('') if dict_info['F77'] is None else str(dict_info['F77'])  # 纬度
                f76 = str('') if dict_info['F78'] is None else str(dict_info['F78'])  # 风向
                f77 = str('') if dict_info['F79'] is None else str(dict_info['F79'])  # 风速
                f78 = str('') if dict_info['F80'] is None else str(dict_info['F80'])  # 正磷酸盐
                f79 = str('')  # 总磷(湖/库)
                f80 = str('')  # 生物毒性
                f81 = str('')  # 氨氮核定值
                f82 = str('')  # 出口压力

                file_obj.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'
                               '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'
                               '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'
                               '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'
                               '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'
                               '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'
                               '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'
                               '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'
                               '%s\t%s\n' %
                               (station_code, station_name, record_time,
                                f01, f02, f03, f04, f05, f06, f07, f08, f09, f10,
                                f11, f12, f13, f14, f15, f16, f17, f18, f19, f20,
                                f21, f22, f23, f24, f25, f26, f27, f28, f29, f30,
                                f31, f32, f33, f34, f35, f36, f37, f38, f39, f40,
                                f41, f42, f43, f44, f45, f46, f47, f48, f49, f50,
                                f51, f52, f53, f54, f55, f56, f57, f58, f59, f60,
                                f61, f62, f63, f64, f65, f66, f67, f68, f69, f70,
                                f71, f72, f73, f74, f75, f76, f77, f78, f79, f80,
                                f81, f82))

            file_obj.seek(0)
            self.pg.copy_from(file_obj, 'tmp_water_quality_data')
            self.pg.commit()
            self.pg.CreateIndex_ByName('idx_tmp_water_quality_data_station_no_record_time')
        except Exception as e:
            print(e)
            raise
        finally:
            file_obj.close()

    def update_related_tbl(self):
        self.log.info('update related table...')
        sql = """
                INSERT INTO water_station (
                    station_no, station_name, drainage_code, area_code, address, 
                    station_type, monitor_level, longitude, latitude
                )
                (
                    SELECT a.station_no, a.station_name, a.drainage_code, a.area_code, a.address, 
                        a.station_type, a.monitor_level, a.longitude, a.latitude
                    FROM tmp_water_station AS a
                    LEFT JOIN water_station AS b
                        ON a.station_no = b.station_no
                    WHERE b.station_no IS NULL
                );
                ANALYZE water_station;
            """
        self.pg.execute(sql)
        self.pg.commit()

        sql = """
                INSERT INTO water_quality_data (
                    station_no, station_name, record_time,
                    f01, f02, f03, f04, f05, f06, f07, f08, f09, f10,
                    f11, f12, f13, f14, f15, f16, f17, f18, f19, f20,
                    f21, f22, f23, f24, f25, f26, f27, f28, f29, f30,
                    f31, f32, f33, f34, f35, f36, f37, f38, f39, f40,
                    f41, f42, f43, f44, f45, f46, f47, f48, f49, f50,
                    f51, f52, f53, f54, f55, f56, f57, f58, f59, f60,
                    f61, f62, f63, f64, f65, f66, f67, f68, f69, f70,
                    f71, f72, f73, f74, f75, f76, f77, f78, f79, f80,
                    f81, f82
                )
                (
                    SELECT a.station_no, a.station_name, a.record_time,
                        a.f01, a.f02, a.f03, a.f04, a.f05, a.f06, a.f07, a.f08, a.f09, a.f10,
                        a.f11, a.f12, a.f13, a.f14, a.f15, a.f16, a.f17, a.f18, a.f19, a.f20,
                        a.f21, a.f22, a.f23, a.f24, a.f25, a.f26, a.f27, a.f28, a.f29, a.f30,
                        a.f31, a.f32, a.f33, a.f34, a.f35, a.f36, a.f37, a.f38, a.f39, a.f40,
                        a.f41, a.f42, a.f43, a.f44, a.f45, a.f46, a.f47, a.f48, a.f49, a.f50,
                        a.f51, a.f52, a.f53, a.f54, a.f55, a.f56, a.f57, a.f58, a.f59, a.f60,
                        a.f61, a.f62, a.f63, a.f64, a.f65, a.f66, a.f67, a.f68, a.f69, a.f70,
                        a.f71, a.f72, a.f73, a.f74, a.f75, a.f76, a.f77, a.f78, a.f79, a.f80,
                        a.f81, a.f82
                    FROM tmp_water_quality_data AS a
                    LEFT JOIN water_quality_data AS b
                        ON a.station_no = b.station_no AND a.record_time = b.record_time
                    WHERE b.station_no IS NULL AND a.record_time IS NOT NULL
                );
                ANALYZE water_quality_data;
            """
        self.pg.execute(sql)
        self.pg.commit()
