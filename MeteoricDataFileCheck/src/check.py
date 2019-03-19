# -*- coding: UTF8 -*-
"""
Created on 2019-03-04

@author: liushengqiang
"""


import paramiko
import datetime
from common import config
from common import log


class CCheck(object):
    __instance = None

    @staticmethod
    def instance():
        if CCheck.__instance is None:
            CCheck.__instance = CCheck()

        return CCheck.__instance

    def __init__(self):
        self.sftp_host = config.CConfigReader.instance().getPara('host')
        self.sftp_port = config.CConfigReader.instance().getPara('port')
        self.sftp_username = config.CConfigReader.instance().getPara('username')
        self.sftp_password = config.CConfigReader.instance().getPara('password')
        self.log = log.CLog.instance().logger("Check")

    def run(self):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.sftp_host, self.sftp_port, self.sftp_username, self.sftp_password)
            sftp = ssh.open_sftp()
            remote_dir = '/home/tmp/rainfall'
            seven_day_ago = datetime.datetime.now() - datetime.timedelta(days=7)
            delta = datetime.timedelta(hours=1)
            offset = datetime.datetime.now()
            remote_files = sftp.listdir(remote_dir)
            missing_files = []
            while offset > seven_day_ago:
                meteorological_element_file = offset.strftime("%y%m%d%H") + ".000"
                meteorological_forecast_file = offset.strftime("%Y%m%d") + "20_072_Fcst.txt"
                if meteorological_element_file not in remote_files:
                    self.log.error("%s", meteorological_element_file)

                if meteorological_forecast_file not in missing_files and \
                        meteorological_forecast_file not in remote_files:
                    missing_files.append(meteorological_forecast_file)
                    self.log.error("%s", meteorological_forecast_file)

                offset -= delta
        except Exception as e:
            print(e)
            raise
        finally:
            sftp.close()
            ssh.close()
