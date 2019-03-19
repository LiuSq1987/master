# -*- coding: UTF8 -*-
"""
Created on 2019-02-20

@author: liushengqiang
"""



import psycopg2
import time
import datetime
from common import config
from common import sql
from common import log

BATCH_SIZE = 1024 * 10


class pg_client(object):
    __instance = None

    @staticmethod
    def instance():
        if pg_client.__instance is None:
            pg_client.__instance = pg_client()

        return pg_client.__instance

    def __init__(self, is_cursor_dict=False):
        try:
            self.connected = False
            self.is_cursor_dict = is_cursor_dict
            self.log = log.CLog.instance().logger("Database")
            self.srv_path = config.CConfigReader.instance().getPara("db")
            if self.srv_path == '':
                self.log.error("Doesn't exist host IP")
                exit(1)

            self.objSqlLoader = sql.CSqlLoader()
            self.objSqlLoader.init()
        except Exception as e:
            raise

    def connect(self):
        """connect to database"""
        try:
            self.conn = psycopg2.connect(self.srv_path, connect_timeout=3)
            self.pgcur = self.conn.cursor()
            self.connected = True
        except Exception as e:
            raise

    def commit(self):
        self.conn.commit()

    def execute(self, sqlcmd, parameters=[]):
        """execute commands"""
        try:
            if parameters:
                self.pgcur.execute(sqlcmd, parameters)
            else:
                self.pgcur.execute(sqlcmd)
            return 0
        except Exception as e:
            raise

    def fetchall(self):
        return self.pgcur.fetchall()

    def copy_from(self, file_obj, table, sep='\t', null="", size=8192, columns=None):
        try:
            self.pgcur.copy_from(file_obj, table, sep, null, size, columns)
        except Exception as e:
            raise

    def batch_query(self, sql, parameters=(), batch_size=BATCH_SIZE):
        try:
            time.sleep(0.01)
            str_time = datetime.datetime.now().strftime("%m-%d_%H-%M-%S-%f")
            curr_name = 'batch-' + str_time
            if self.is_cursor_dict:
                curs = self.conn.cursor(name=curr_name, cursor_factory=psycopg2.extras.DictCursor)
            else:
                curs = self.conn.cursor(name=curr_name)
            if parameters:
                curs.execute(sql, parameters)
            else:
                curs.execute(sql)
            while True:
                rows = curs.fetchmany(size=batch_size)
                if not rows:
                    break
                yield rows
            curs.close()
        except Exception as e:
            raise

    def get_batch_data(self, sqlcmd, parameters=(), batch_size=BATCH_SIZE):
        try:
            row_data = list()
            for rows in self.batch_query(sqlcmd, parameters, batch_size):
                for row in rows:
                    if not row:
                        break
                    row_data = row
                    yield row_data
        except Exception as e:
            raise

    def close(self):
        if self.connected:
            self.pgcur.close()
            self.conn.close()
            self.connected = False

    def fetchone(self):
        """read one record"""
        return self.pgcur.fetchone()

    def fetchall(self):
        return self.pgcur.fetchall()

    def GetDropFKeyStr(self, fkey_name, table_name):
        if fkey_name is None or fkey_name == '':
            return ''

        if table_name is None or table_name == '':
            return ''

        return "ALTER TABLE " + table_name + " DROP CONSTRAINT if exists " + fkey_name + ";\n"

    def DropFKey_ByName(self, table_name):
        """drop foreign key of table"""
        if not self.connected:
            self.log.error("Connect is not opened.")
            return -1

        if table_name is None or table_name == '':
            self.log.error("table_name is None.")
            return -1

        # find all fkey, which reference "table_name"
        sqlcmd = """
                select  b.conname as fkey_name, C.relname as table_name
                from (
                    SELECT relid, relname
                      FROM pg_statio_user_tables
                      where relname = %s
                ) AS A
                left join pg_constraint as B
                ON B.confrelid = A.relid
                left join pg_statio_user_tables as C
                ON B.conrelid = C.relid
                """
        self.execute(sqlcmd, (table_name,))
        row = self.fetchone()
        sqlcmd = ''
        while row:
            sqlcmd += self.GetDropFKeyStr(row[0], row[1])
            row = self.fetchone()

        # Drop all fkey, which reference "table_name"
        if sqlcmd != '':
            if self.execute(sqlcmd, table_name) == -1:
                return -1
            else:
                self.commit()

        return 0

    def GetDropViewStr(self, view_name):
        if view_name is None or view_name == '':
            return ''

        return "DROP VIEW " + view_name + ";\n"

    def DropView_ByName(self, table_name):
        """drop view of table"""
        if not self.connected:
            self.log.error("Connect is not opened.")
            return -1

        if table_name is None or table_name == '':
            self.log.error("table_name is None.")
            return -1

        # find all views, which reference "table_name"
        sqlcmd = """
                SELECT viewname
                 FROM pg_views
                 where definition like %s or definition like %s or definition like %s;
                 """

        if self.execute(sqlcmd, ("%" + table_name + " %", "%" + table_name + ",%", "%" + table_name + ")%")) == -1:
            return -1
        row = self.fetchone()
        sqlcmd = ''
        while row:
            sqlcmd += self.GetDropViewStr(row[0])
            row = self.fetchone()

        # Drop all views, which reference "table_name"
        if sqlcmd != '':
            if self.execute(sqlcmd, table_name) == -1:
                return -1
            else:
                self.commit()

        return 0

    def CreateTable(self, sqlcmd, table_name):
        """create tables"""

        sqlstr = '''
                 SELECT  tablename
                 FROM  pg_tables 
                 WHERE tablename = %s
                 '''
        if self.execute(sqlstr, (table_name,)) == -1:
            self.log.error('Search table  ' + table_name + ' failed :' + sqlstr)
            return -1
        else:
            if self.pgcur.rowcount == 1:  # table already exist
                # delete foreign keys of table
                self.DropFKey_ByName(table_name)
                # delete views of table
                self.DropView_ByName(table_name)
                try:
                    self.execute('DROP TABLE ' + table_name)
                    self.commit()
                except:
                    self.log.error('DROP TABLE  ' + table_name + 'failed.')
                    raise

        try:
            # self.log.info('Now it is creating table ' + table_name +'...')
            if self.execute(sqlcmd) == -1:
                # self.log.error('Create table ' + table_name + 'failed :' + sqlcmd)
                return -1
            self.commit()
            return 0
        except:
            self.log.error('Create table ' + table_name + 'failed :' + sqlcmd)
            raise

    def CreateTable_ByName(self, table_name):
        if not self.connected:
            self.log.error("Connect is not opened.")
            return -1

        sqlcmd = self.objSqlLoader.GetSqlScript(table_name)
        if sqlcmd is None:
            self.log.error("Doesn't exist SQL script for table " + table_name)
            return -1

        return self.CreateTable(sqlcmd, table_name)

    def IsExistTable(self, table_name):
        if not self.connected:
            self.log.error("Connect is not opened.")
            return False

        sql_str = """
                SELECT tablename
                FROM pg_tables
                WHERE tablename = %s
            """
        if self.execute(sql_str, (table_name,)) == -1:
            return False
        else:
            if self.pgcur.rowcount == 1:  # table already exist
                return True

        return False

    def create_index(self, sqlcmd, index_name):
        """create indexes"""
        # check the index
        sqlstr = """select * from pg_indexes
                    where  indexname = %s
                 """

        try:
            # search the index by name
            self.pgcur.execute(sqlstr, (index_name,))
            if self.pgcur.rowcount == 1:
                # self.log.warning('Has been existed index "' + index_name + '"')
                return 0
            else:
                try:
                    self.pgcur.execute(sqlcmd)
                    self.conn.commit()
                    # self.log.info('Created index index "' + index_name + '"')
                    return 0
                except:
                    self.log.error('Created index ' + index_name + ' failed :' + sqlcmd)
                    raise
        except:
            self.log.error('Created index ' + index_name + ' failed :' + sqlcmd)
            raise

    def CreateIndex_ByName(self, index_name):
        if not self.connected:
            self.log.error("Connect is not opened.")
            return -1

        sqlcmd = self.objSqlLoader.GetSqlScript(index_name)
        if sqlcmd is None:
            # self.log.error("Doesn't exist SQL script for index " +  index_name)
            return -1
        return self.create_index(sqlcmd, index_name)

    def CreateFunction_ByName(self, function_name):
        if not self.connected:
            self.log.error("Connect is not opened.")
            return -1

        sql_cmd = self.objSqlLoader.GetSqlScript(function_name)
        if sql_cmd is None:
            self.log.error("Doesn't exist SQL script for function " + function_name)
            return -1

        if self.execute(sql_cmd) == -1:
            return -1

        self.conn.commit()

        return 0
