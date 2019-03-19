# -*- coding: UTF8 -*-
"""
Created on 2019-03-05

@author: liushengqiang
"""


import paramiko
import os
# from decimal import *
from common import log
from common import config
from common import database
from common import common


class CMeteorologicalBaseData(object):
    def __init__(self):
        self.log = log.CLog.instance().logger('Meteorological Base Data')
        self.config = config.CConfigReader.instance()

        # SFTP Server Param
        meteorological_data_path = self.config.getPara('meteorological_data_path')
        paras = meteorological_data_path.split(' ')
        self.sftp_hostname = paras[0][paras[0].find('=') + 1:].strip()
        self.sftp_port = paras[1][paras[1].find('=') + 1:].strip()
        self.sftp_username = paras[2][paras[2].find('=') + 1:].strip()
        self.sftp_password = paras[3][paras[3].find('=') + 1:].strip()

        # 数据库
        self.pg = database.pg_client()

        # 预收录记录
        self.element_files = []
        self.forecast_files = []

        # 已收录记录
        self.element_time_included = []
        self.forecast_time_included = []

    def do(self):
        try:
            # 连接数据库
            self.pg.connect()

            self.prepare()
            self.copy_station_data()
            self.load_meteorological_base_data()
            self.update_related_tbl()

        except Exception as e:
            print(e)
            raise
        finally:
            self.pg.close()

    def copy_station_data(self):
        try:
            self.log.info("copy station data begin...")
            import xlrd
            workbook = xlrd.open_workbook("./docs/安徽省巢湖流域气象站点信息数据总汇.xls")
            self.pg.CreateTable_ByName('tmp_meteorological_station')
            file_obj = common.open("station_data")
            for sheet in workbook.sheets():
                for row in range(sheet.nrows):
                    if 0 == row:
                        continue
                    station_no = str('')
                    if sheet.cell(row, 0).value is not None:
                        station_no = str(int(sheet.cell(row, 0).value)) if isinstance(sheet.cell(row, 0).value, float) \
                            else str(sheet.cell(row, 0).value)
                    station_name = str('') if sheet.cell(row, 1).value is None else str(sheet.cell(row, 1).value)
                    lon = str('') if sheet.cell(row, 2).value is None else str(sheet.cell(row, 2).value)
                    lat = str('') if sheet.cell(row, 3).value is None else str(sheet.cell(row, 3).value)
                    file_obj.write('%s\t%s\t%s\t%s\n' %
                                   (station_no, station_name, lon, lat))
            file_obj.seek(0)
            self.pg.copy_from(file_obj, 'tmp_meteorological_station')
            self.pg.commit()
        except Exception as e:
            self.log.error("exception happened in copy station data---%s", e)
            raise
        finally:
            common.close(file_obj, True)

    def prepare(self):
        if not self.pg.IsExistTable('meteorological_element_data'):
            self.pg.CreateTable_ByName('meteorological_element_data')
            self.pg.CreateIndex_ByName('idx_meteorological_element_data_station_no_record_time')

        if not self.pg.IsExistTable('meteorological_forecast_data'):
            self.pg.CreateTable_ByName('meteorological_forecast_data')
            self.pg.CreateIndex_ByName('idx_meteorological_forecast_data_city_record_time')

        if not self.pg.IsExistTable('meteorological_station'):
            self.pg.CreateTable_ByName('meteorological_station')
            self.pg.CreateIndex_ByName('idx_meteorological_station_station_no')

        sql = """
                select distinct record_time
                from meteorological_element_data
            """
        self.pg.execute(sql)
        rows = self.pg.fetchall()
        for row in rows:
            reporting_time = row[0]
            reporting_time = reporting_time.strftime("%y%m%d%H")
            self.element_time_included.append(reporting_time)

        sql = """
                select distinct record_time 
                from meteorological_forecast_data
            """
        self.pg.execute(sql)
        rows = self.pg.fetchall()
        for row in rows:
            reporting_time = row[0]
            reporting_time = reporting_time.strftime("%Y%m%d%H")
            self.forecast_time_included.append(reporting_time)

    def load_meteorological_base_data(self):
        try:
            # 创建SSH对象
            ssh = paramiko.SSHClient()
            # 允许连接不在know_hosts文件中的主机
            # 第一次登录的认证信息
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # 连接服务器
            ssh.connect(self.sftp_hostname, self.sftp_port, self.sftp_username, self.sftp_password)
            sftp = ssh.open_sftp()

            temp_folder = os.path.join(self.config.getPara('cache_path'))
            try:
                os.stat(temp_folder)
            except:
                os.makedirs(temp_folder)

            remote_dir = '/home/tmp/rainfall'
            remote_files = sftp.listdir(remote_dir)
            for remote_file in remote_files:
                (name, extension) = os.path.splitext(remote_file)
                if extension.lower() == ".000" and name in self.element_time_included:
                    continue
                elif extension.lower() == ".txt" and name.find("Fcst") > 0 and name[:10] in self.forecast_time_included:
                    continue

                temp_file = remote_dir + '/' + remote_file
                local_file = temp_folder + '/' + remote_file
                sftp.get(temp_file, local_file)
                if extension.lower() == ".000":
                    self.element_files.append(local_file)
                elif extension.lower() == ".txt" and name.find("Fcst") > 0:
                    self.forecast_files.append(local_file)
                else:
                    self.log.warning("The file is not meteorological data---%s", remote_file)
                    os.remove(local_file)

            self.copy_element_data()
            self.copy_forecast_data()
        except Exception as e:
            print(e)
            raise
        finally:
            sftp.close()
            ssh.close()

    def copy_element_data(self):
        try:
            self.log.info("copy element data begin...")
            self.pg.CreateTable_ByName('tmp_meteorological_element_data')
            file_obj = common.open("element_data")
            for element_file in self.element_files:
                tmp_file_obj = open(element_file, 'r')
                lines = tmp_file_obj.readlines()
                tmp_file_obj.close()

                times = str(lines[1][:20].strip()).split(" ")
                reporting_time = "%s-%s-%s %s:%s:%s" % (times[0], times[1], times[2], times[3], times[4], times[5])

                if lines[1].find("1hour") > 0:
                    temp_lines = lines[3:]
                else:
                    temp_lines = lines[4:]

                for line in temp_lines:
                    data = line[:-1].split("\t")
                    station_no = str('') if data[0] is None else str(data[0])
                    station_name = str('') if data[1] is None else str(data[1])
                    lon = str('') if data[2] is None else str(data[2])
                    lat = str('') if data[3] is None else str(data[3])
                    precipitation = str('') if data[4] is None else str(data[4])
                    if len(data) > 5:
                        temperature = str('') if data[5] is None else str(data[5])
                        wind_dir = str('') if data[6] is None else str(data[6])
                        wind_spd = str('') if data[7] is None else str(data[7])
                        evaporation = str('') if data[8] is None else str(data[8])
                    else:
                        temperature = str('')
                        wind_dir = str('')
                        wind_spd = str('')
                        evaporation = str('')

                    file_obj.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %
                                   (reporting_time, station_no, station_name, lon, lat,
                                    precipitation, temperature, wind_dir, wind_spd, evaporation))
                os.remove(element_file)

            file_obj.seek(0)
            self.pg.copy_from(file_obj, 'tmp_meteorological_element_data')
            self.pg.commit()
            self.pg.CreateIndex_ByName('idx_tmp_meteorological_element_data_station_no_record_time')
        except Exception as e:
            self.log.error("exception happened in copy element data---%s", e)
            raise
        finally:
            common.close(file_obj, True)

    def copy_forecast_data(self):
        try:
            self.log.info("copy forecast data begin...")
            self.pg.CreateTable_ByName('tmp_meteorological_forecast_data')
            file_obj = common.open("forecast_data")
            for forecast_file in self.forecast_files:
                tmp_file_obj = open(forecast_file, 'r')
                lines = tmp_file_obj.readlines()
                tmp_file_obj.close()
                if lines[1].find("起报时间:") < 0:
                    self.log.error("forecast data error---%s", forecast_file)
                    continue
                times = str(lines[1][lines[1].find(":")+1:-1].strip()).split(" ")
                reporting_time = "%s-%s-%s %s:00:00" % (times[0], times[1], times[2], times[3])
                for line in lines[3:]:
                    data = line.split("\t")
                    city = str('') if data[0] is None else str(data[0])
                    high_temperature_24 = str('') if data[1] is None else str(data[1])
                    low_temperature_24 = str('') if data[2] is None else str(data[2])
                    weather_12 = str('') if data[3] is None else str(data[3])
                    weather_24 = str('') if data[4] is None else str(data[4])
                    wind_dir_12 = str('') if data[5] is None else str(data[5])
                    wind_spd_12 = str('') if data[6] is None else str(data[6])
                    wind_dir_24 = str('') if data[7] is None else str(data[7])
                    wind_spd_24 = str('') if data[8] is None else str(data[8])
                    high_temperature_48 = str('') if data[9] is None else str(data[9])
                    low_temperature_48 = str('') if data[10] is None else str(data[10])
                    weather_36 = str('') if data[11] is None else str(data[11])
                    weather_48 = str('') if data[12] is None else str(data[12])
                    wind_dir_36 = str('') if data[13] is None else str(data[13])
                    wind_spd_36 = str('') if data[14] is None else str(data[14])
                    wind_dir_48 = str('') if data[15] is None else str(data[15])
                    wind_spd_48 = str('') if data[16] is None else str(data[16])
                    high_temperature_72 = str('') if data[17] is None else str(data[17])
                    low_temperature_72 = str('') if data[18] is None else str(data[18])
                    weather_60 = str('') if data[19] is None else str(data[19])
                    weather_72 = str('') if data[20] is None else str(data[20])
                    wind_dir_60 = str('') if data[21] is None else str(data[21])
                    wind_spd_60 = str('') if data[22] is None else str(data[22])
                    wind_dir_72 = str('') if data[23] is None else str(data[23])
                    wind_spd_72 = str('') if data[24] is None else str(data[24])

                    file_obj.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'
                                   '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %
                                   (reporting_time, city,
                                    high_temperature_24, low_temperature_24, weather_12, weather_24,
                                    wind_dir_12, wind_spd_12, wind_dir_24, wind_spd_24,
                                    high_temperature_48, low_temperature_48, weather_36, weather_48,
                                    wind_dir_36, wind_spd_36, wind_dir_48, wind_spd_48,
                                    high_temperature_72, low_temperature_72, weather_60, weather_72,
                                    wind_dir_60, wind_spd_60, wind_dir_72, wind_spd_72))

                os.remove(forecast_file)

            file_obj.seek(0)
            self.pg.copy_from(file_obj, 'tmp_meteorological_forecast_data')
            self.pg.commit()
            self.pg.CreateIndex_ByName('idx_tmp_meteorological_forecast_data_city_record_time')
        except Exception as e:
            self.log.error("exception happened in copy forecast data---%s", e)
            raise
        finally:
            common.close(file_obj, True)

    def update_related_tbl(self):
        self.log.info('update related table...')
        if -1 == self.pg.CreateFunction_ByName('check_char_valid'):
            return -1

        sql = """
                INSERT INTO meteorological_station (
                    station_no, station_name, longitude, latitude
                )
                (
                    SELECT a.station_no, 
                        (CASE 
                            WHEN b.station_name IS NOT NULL THEN b.station_name ELSE a.station_name 
                        END) AS station_name,
                        (CASE WHEN b.longitude IS NOT NULL THEN b.longitude ELSE a.longitude END) AS longitude,
                        (CASE WHEN b.latitude IS NOT NULL THEN b.latitude ELSE a.latitude END) AS latitude
                    FROM (
                        SELECT station_no, (ARRAY_AGG(station_name))[1] AS station_name, 
                            (ARRAY_AGG(longitude))[1] AS longitude, (ARRAY_AGG(latitude))[1] AS latitude
                        FROM (
                            SELECT DISTINCT station_no, longitude, char_length(longitude::varchar), 
                                latitude, char_length(latitude::varchar),
                                (CASE 
                                    WHEN not check_char_valid(station_name) THEN NULL 
                                    ELSE station_name 
                                END) AS station_name
                            FROM tmp_meteorological_element_data
                            ORDER BY station_no, station_name, char_length(longitude::varchar) DESC, longitude DESC, 
                                char_length(latitude::varchar) DESC, latitude DESC
                        ) AS a
                        GROUP BY station_no
                    ) AS a
                    LEFT JOIN tmp_meteorological_station AS b
                        ON a.station_no = b.station_no
                    LEFT JOIN meteorological_station AS c
                        ON a.station_no = c.station_no
                    WHERE c.station_no IS NULL
                );
                ANALYZE meteorological_station;
            """
        self.pg.execute(sql)
        self.pg.commit()

        sql = """
                INSERT INTO meteorological_element_data (
                    record_time, station_no, station_name, longitude, latitude, 
                    precipitation, temperature, wind_direction, wind_speed, evaporation
                )
                (
                    SELECT DISTINCT a.record_time, a.station_no, c.station_name, c.longitude, c.latitude,
                        (CASE WHEN a.precipitation > 300 THEN NULL ELSE a.precipitation END) AS precipitation,
                        (CASE WHEN a.temperature > 100 THEN NULL ELSE a.temperature END) AS temperature,
                        (CASE WHEN a.wind_direction > 3600 THEN NULL ELSE a.wind_direction % 360 END) AS wind_direction,
                        (CASE WHEN a.wind_speed > 100 THEN NULL ELSE a.wind_speed END) AS wind_speed,
                        (CASE WHEN a.evaporation > 100 THEN NULL ELSE a.evaporation END) AS evaporation
                    FROM tmp_meteorological_element_data AS a
                    LEFT JOIN meteorological_station AS c
                        ON a.station_no = c.station_no
                    LEFT JOIN meteorological_element_data AS b
                        ON a.station_no = b.station_no AND a.record_time = b.record_time
                    WHERE b.station_no IS NULL
                );
                ANALYZE meteorological_element_data;
            """
        self.pg.execute(sql)
        self.pg.commit()

        sql = """
                INSERT INTO meteorological_forecast_data ( 
                    record_time, city, 
                    high_temperature_24, low_temperature_24, weather_12, weather_24, 
                    wind_direction_12, wind_speed_12, wind_direction_24, wind_speed_24, 
                    high_temperature_48, low_temperature_48, weather_36, weather_48, 
                    wind_direction_36, wind_speed_36, wind_direction_48, wind_speed_48, 
                    high_temperature_72, low_temperature_72, weather_60, weather_72, 
                    wind_direction_60, wind_speed_60, wind_direction_72, wind_speed_72
                )
                (
                    SELECT a.record_time, a.city, 
                        (CASE WHEN a.high_temperature_24 > 100 THEN NULL ELSE a.high_temperature_24 END) AS high_temperature_24,
                        (CASE WHEN a.low_temperature_24 > 100 THEN NULL ELSE a.low_temperature_24 END) AS low_temperature_24,
                        (CASE WHEN not check_char_valid(a.weather_12) THEN NULL ELSE a.weather_12 END) AS weather_12,
                        (CASE WHEN not check_char_valid(a.weather_24) THEN NULL ELSE a.weather_24 END) AS weather_24,
                        (CASE WHEN not check_char_valid(a.wind_direction_12) THEN NULL ELSE a.wind_direction_12 END) AS wind_direction_12,
                        (CASE WHEN not check_char_valid(a.wind_speed_12) THEN NULL ELSE a.wind_speed_12 END) AS wind_speed_12,
                        (CASE WHEN not check_char_valid(a.wind_direction_24) THEN NULL ELSE a.wind_direction_24 END) AS wind_direction_24,
                        (CASE WHEN not check_char_valid(a.wind_speed_24) THEN NULL ELSE a.wind_speed_24 END) AS wind_speed_24,
                        (CASE WHEN a.high_temperature_48 > 100 THEN NULL ELSE a.high_temperature_48 END) AS high_temperature_48,
                        (CASE WHEN a.low_temperature_48 > 100 THEN NULL ELSE a.low_temperature_48 END) AS low_temperature_48,
                        (CASE WHEN not check_char_valid(a.weather_36) THEN NULL ELSE a.weather_36 END) AS weather_36,
                        (CASE WHEN not check_char_valid(a.weather_48) THEN NULL ELSE a.weather_48 END) AS weather_48,
                        (CASE WHEN not check_char_valid(a.wind_direction_36) THEN NULL ELSE a.wind_direction_36 END) AS wind_direction_36,
                        (CASE WHEN not check_char_valid(a.wind_speed_36) THEN NULL ELSE a.wind_speed_36 END) AS wind_speed_36,
                        (CASE WHEN not check_char_valid(a.wind_direction_48) THEN NULL ELSE a.wind_direction_48 END) AS wind_direction_48,
                        (CASE WHEN not check_char_valid(a.wind_speed_48) THEN NULL ELSE a.wind_speed_48 END) AS wind_speed_48,
                        (CASE WHEN a.high_temperature_72 > 100 THEN NULL ELSE a.high_temperature_72 END) AS high_temperature_72,
                        (CASE WHEN a.low_temperature_72 > 100 THEN NULL ELSE a.low_temperature_72 END) AS low_temperature_72,
                        (CASE WHEN not check_char_valid(a.weather_60) THEN NULL ELSE a.weather_60 END) AS weather_60,
                        (CASE WHEN not check_char_valid(a.weather_72) THEN NULL ELSE a.weather_72 END) AS weather_72,
                        (CASE WHEN not check_char_valid(a.wind_direction_60) THEN NULL ELSE a.wind_direction_60 END) AS wind_direction_60,
                        (CASE WHEN not check_char_valid(a.wind_speed_60) THEN NULL ELSE a.wind_speed_60 END) AS wind_speed_60,
                        (CASE WHEN not check_char_valid(a.wind_direction_72) THEN NULL ELSE a.wind_direction_72 END) AS wind_direction_72,
                        (CASE WHEN not check_char_valid(a.wind_speed_72) THEN NULL ELSE a.wind_speed_72 END) AS wind_speed_72
                    FROM tmp_meteorological_forecast_data AS a
                    LEFT JOIN meteorological_forecast_data AS b
                        ON a.city = b.city AND a.record_time = b.record_time
                    WHERE b.city IS NULL
                );
                ANALYZE meteorological_forecast_data;
            """
        self.pg.execute(sql)
        self.pg.commit()
