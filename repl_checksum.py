# -*- coding:utf-8 -*-
# edit by fuzongfei
# python3.6

import argparse
import configparser
import socket
import smtplib
import subprocess
import sys
import time
from email.mime.text import MIMEText
from os.path import isfile
import logging

from collections import namedtuple
import pymysql
from warnings import filterwarnings
filterwarnings("error",category=pymysql.Warning)
import os
# 在所在的目录建立report目录并进入
DIR = sys.path[0]
os.chdir(DIR)

current_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())


def loggingconf(filename):
    logging.basicConfig(level=logging.DEBUG,  
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  
                        datefmt='%a, %d %b %Y %H:%M:%S',  
                        filename='/tmp/' + filename+ '.log',  
                        filemode='w')


hostname = socket.gethostname()

check_statistics = []
check_statistics.append({'开始校验': current_time})
check_statistics.append({'主机名': hostname})

def get_arguments():
    try:
        parser = argparse.ArgumentParser(description='This a auto pt-table-checksum help document.')
        parser.add_argument('-f', '--file', type=str, help='this pt-table-checksum config file.')
        args = parser.parse_args()
        return args
    except Exception as Err:
        print ('Err: ',Err)


class SQLgo(object):
    def __init__(self, ip=None, user=None, password=None, port=None, db=None):
        self.ip = ip
        self.user = user
        self.password = password
        self.db = db
        self.port = int(port)
        self.con = object

    def __enter__(self):
        self.con = pymysql.connect(
            host=self.ip,
            user=self.user,
            passwd=self.password,
            db=self.db,
            charset='utf8mb4',
            port=self.port
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()

    def execute(self, sql=None):
        with self.con.cursor() as cursor:
            sqllist = sql
            cursor.execute(sqllist)
            result = cursor.fetchall()
            self.con.commit()
        return result

    def tablename(self):
        with self.con.cursor() as cursor:
            cursor.execute('show tables')
            result = cursor.fetchall()
            data = [c for i in result for c in i]
            return data
    # def showtables(self):
    #     with self.con.cursor() as cursor:
    #         sql = '''
    #             SELECT
    #                 t.TABLE_SCHEMA,
    #                 t.TABLE_NAME 
    #             FROM
    #                 information_schema.TABLES t
    #                 INNER JOIN information_schema.statistics s ON t.TABLE_SCHEMA = s.TABLE_SCHEMA 
    #                 AND t.TABLE_NAME = s.TABLE_NAME
    #                 INNER JOIN information_schema.key_column_usage k ON t.TABLE_SCHEMA = k.TABLE_SCHEMA 
    #                 AND t.TABLE_NAME = k.TABLE_NAME 
    #             WHERE
    #                 t.TABLE_TYPE = 'BASE TABLE' 
    #                 AND t.ENGINE = 'InnoDB' 
    #                 AND s.NON_UNIQUE = 0 
    #                 AND k.POSITION_IN_UNIQUE_CONSTRAINT IS NULL 
    #                 AND concat( k.TABLE_SCHEMA, '.', k.TABLE_NAME ) NOT IN ( SELECT concat( k.TABLE_SCHEMA, '.', k.TABLE_NAME ) FROM information_schema.key_column_usage k WHERE k.POSITION_IN_UNIQUE_CONSTRAINT IS NOT NULL ) 
    #                 AND t.TABLE_SCHEMA NOT IN ( 'mysql', 'percona', 'sys', 'information_schema', 'performance_schema' ) 
    #             GROUP BY
    #                 t.TABLE_SCHEMA,
    #                 t.TABLE_NAME;
    #         '''
    #         cursor.execute(sql)
    #         result = cursor.fetchall()
    #         td = [ {'database': i[0], 'table': i[1],} for i in result ]
    #     return td



def conf_path(file) -> object:
    '''
    读取配置文件属性
    '''
    if isfile(file):
        _conf = configparser.ConfigParser(allow_no_value=True)
        _conf.read(file)
        conf_set = namedtuple(
            "name",
            ["master_host", "master_port", "master_user", "master_pass", "remote_host", "check_user",
         "check_pass", "slave_hosts", "pt_table_checksum", "pt_table_sync", "databases", "dsndb",
         "mail_user","mail_password","smtp_host", "smtp_port", "mail_sender", "mail_receiver"])

        return conf_set(_conf.get('mysql', 'master_host'),
                    _conf.get('mysql', 'master_port'),
                    _conf.get('mysql', 'master_user'),
                    _conf.get('mysql', 'master_pass'),
                    _conf.get('mysql', 'remote_host'),
                    _conf.get('mysql', 'check_user'),
                    _conf.get('mysql', 'check_pass'),
                    _conf.get('mysql', 'slave_hosts'),
                    _conf.get('mysql', 'pt_table_checksum'),
                    _conf.get('mysql', 'pt_table_sync'),
                    _conf.get('mysql', 'databases'),
                    _conf.get('mysql', 'dsndb'),
                    _conf.get('email', 'username'),
                    _conf.get('email', 'password'),
                    _conf.get('email', 'smtp_server'),
                    _conf.get('email', 'smtp_port'),
                    _conf.get('email', 'mail_sender'),
                    _conf.get('email', 'mail_receiver'),
                    )




class CheckSums(object):

    def __init__(self, conf, database, table):
        self.conf = conf
        self.database = database
        self.table = table
        self.sync_cmd = ""
        self.cmd = '''%s -u%s -p'%s' -h%s -P%s \
            --recursion-method dsn=D=%s,t=dsns \
            --replicate %s.checksums -d %s --tables %s \
            --nocheck-replication-filters --no-check-binlog-format \
            --empty-replicate-table --chunk-size=8000 --chunk-size-limit=1000 ''' % (self.conf.pt_table_checksum, self.conf.check_user,
                self.conf.check_pass, self.conf.master_host,
                self.conf.master_port, self.conf.dsndb, self.conf.dsndb,
                self.database, self.table)

    def check(self):
        print(self.cmd)
        logging.info("begin to perform data valid check")
        logging.info(f"ouput command: \n\t{self.cmd}")
        status, output = subprocess.getstatusoutput(self.cmd)
        return {'status': status, 'output': output}

    def diff(self):
        """
        ：使用上一次数据校验存储在--replicate指定的校验信息表的校验数据，来检测主从数据的一致性
        : 只输出数据不一致的表的校验结果
        """
        sync_cmd1=[]
        diff_cmd = '''%s \
        --replicate-check-only
        ''' % (self.cmd)
        logging.info("begin to perform data difference detection")
        logging.info(f"ouput command: \n\t{diff_cmd}")
        print ("diff_cmd:%s" % diff_cmd)
        status, output = subprocess.getstatusoutput(diff_cmd)

        logging.info({'status': status, 'output': output})
        output=output.replace('Checking if all tables can be checksummed ...\n','').replace('Starting checksum ...','')
         
        #如果对比有问题，那么再生成一条pt-table-sync的命令
        if output:
            slave_hosts = self.conf.slave_hosts.split(',')
            for i in slave_hosts:
                slave_host=i.split(':')[0]
                slave_port=i.split(':')[1]
                self.sync_args_options = f" --no-check-slave --replicate {self.conf.dsndb}.checksums -d {self.database} -t {self.table} --sync-to-master" \
                                    f" --print"
                self.sync_cmd = ' '.join(
                    (self.conf.pt_table_sync, f"u={self.conf.check_user},p='{self.conf.check_pass}',h={slave_host},P={slave_port}",
                     self.sync_args_options))
                status1, output1 = subprocess.getstatusoutput(self.sync_cmd)
                if status1 != 0:
                    sync_cmd1.append(self.sync_cmd.replace(self.conf.check_pass,'xxxxxxxx'))


            logging.info("begin to perform data sync detection")
            logging.info(f"output command: \n\t{self.sync_cmd}")

        #输出到邮件的信息
            self.cmd1=self.cmd.replace(self.conf.check_pass,'xxxxxxxx')
            check_statistics.append({
                '主库': self.conf.master_host,
                '从库': self.conf.slave_hosts,
                '被检查的库表': self.database+'.'+self.table,
                '检查语句': self.cmd1
            })

            check_statistics.append({
            '差异化语句生成命令': sync_cmd1,
            })



                # self.sync_cmd = '''%s --print --sync-to-master \
                #     h=%s,P=%s,u=%s,p="%s" --no-check-slave --databases="%s" \
                #     --tables="%s"
                # ''' % (self.conf.pt_table_sync, slave.split(':')[0], slave.split(':')[1], self.conf.check_user,
                #     self.conf.check_pass, self.database, self.table),

                # status1, output1 = subprocess.getstatusoutput(self.sync_cmd)
                # if status1 != 0:
                #     logging.info("begin to perform data sync detection")
                #     logging.info(f"output command: \n\t{self.sync_cmd}")
                #     print ("sync_cmd1:%s" % self.sync_cmd)

                # #输出到邮件的信息
                #     self.cmd1=self.cmd.replace(self.conf.check_pass,'xxxxxxxx')
                #     check_statistics.append({
                #         '主库': self.conf.master_host,
                #         '从库': slave,
                #         '被检查的库表': self.database+'.'+self.table,
                #         '检查语句': self.cmd1
                #     })

                #     sync_cmd1=self.sync_cmd.replace(self.conf.check_pass,'xxxxxxxx')
                #     check_statistics.append({
                #     '差异化语句生成命令': sync_cmd1,
                #     })
           
        return output
           
class SendMail(object):
    def __init__(self, conf):
        self.conf = conf

    def send_mail(self, type, data):
        FMT_INFO = '{} --> {}\n'
        content_list = []
        for i in data:
            for k in i:
                content_list.append(FMT_INFO.format(k, i[k]))
        message = '\n'.join(content_list)
        # mail_msg = """
        #     <p>Python 邮件发送测试...</p>
        #     <p><a href="http://www.runoob.com">这是一个链接</a></p>
        #     """
        msg = MIMEText(message, _subtype='plain', _charset='gb2312')
        msg['Subject'] = '[{status}]_{title}_{master_host}'.format(status='OK' if type == 0 else 'WARN', title='主从数据一致性检测[dss]', master_host=self.conf.master_host)
        msg['From'] = self.conf.mail_sender
        msg['To'] = ";".join(list(self.conf.mail_receiver.split(',')))
        to_all = self.conf.mail_receiver.split(',')
        try:
            server = smtplib.SMTP()
            server.connect(self.conf.smtp_host, self.conf.smtp_port)
            server.ehlo()
            #server.starttls()
            server.login(self.conf.mail_user, self.conf.mail_password)
            server.sendmail(self.conf.mail_sender, to_all, msg.as_string())
            server.close()
        except Exception as err:
            logging.error(err.msg)


def initdata(conf):
    try:
        with SQLgo(
            conf.master_host,
            conf.master_user,
            conf.master_pass,
            conf.master_port,
        ) as f:
            sqllist = []
            sql_1 = '''CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET 'utf8mb4';''' % (conf.dsndb)
            sqllist.append(sql_1)
            sql_2 = '''CREATE TABLE IF NOT EXISTS `%s`.`dsns` (
                        `id` INT (11) NOT NULL AUTO_INCREMENT,
                        `parent_id` INT (11) DEFAULT NULL,
                        `dsn` VARCHAR (255) NOT NULL,
                        PRIMARY KEY (`id`),
                        UNIQUE KEY `uk_dsn` (`dsn`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;''' % conf.dsndb
            sqllist.append(sql_2)
            sql_3 = '''CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED BY '%s';''' % (conf.check_user,conf.remote_host, conf.check_pass)
            sqllist.append(sql_3)

            sql_4 ='''
            TRUNCATE TABLE `%s`.`dsns`;
            ''' % conf.dsndb
            sqllist.append(sql_4)

            sql_5 = '''GRANT SELECT,INSERT,UPDATE,DELETE,CREATE,PROCESS,SUPER,REPLICATION SLAVE ON *.* TO '%s'@'%s';''' % (conf.check_user, conf.remote_host)

            sqllist.append(sql_5)
            slave_hosts = conf.slave_hosts.split(',')
            for i in slave_hosts:
                slave = i.split(':')[0]
                slave_port = i.split(':')[1]
                sql = '''insert ignore into `%s`.dsns(dsn) values('h=%s,u=%s,p=%s,P=%s');''' % (conf.dsndb, slave,
                    conf.check_user, conf.check_pass, slave_port)
                sqllist.append(sql)

            #print ("sqllist:%s" % sqllist)

            for i in sqllist:
                try:
                    print ('sql',i)
                    data = f.execute(i)
                    #print("SQLdata:%s" % data)
                except pymysql.Warning as e:
                    logging.info("%s:%s"  % (i, e)) 
                else:
                    logging.info("%s success" % i)
    except Exception as e:
        print ('sql初始化校验数据失败Err: ',e)

def runchecksums():
    arguments = get_arguments()
    file = arguments.file
    conf = conf_path(file)
    loggingconf(file)
    logging.info('info message')



    initdata(conf)

    error = 0
    for db in conf.databases.split(','):
        type1 = 0
        try:
            with SQLgo(
                conf.master_host,
                conf.master_user,
                conf.master_pass,
                conf.master_port,
                db
            ) as f:
                res = f.tablename()      
                for table in res:
                    print ("%s---%s" % (db, table))
                    #if table =="cb_role":
                    checksum = CheckSums(conf, db, table)
                    check = checksum.check()
                    print ("echo check status:%s" % check['status'])
                    if check['status'] in [0, 1,16, 32, 64]:
                        diff = checksum.diff()
                        if diff:
                            type1 = 1
                            error = 1
                            check_statistics.append({'是否存在差异': '是', '差异输出': '\n'+diff})
                if type1 ==0:
                    check_statistics.append({'检查主机': conf.master_host})
                    check_statistics.append({'检查数据库': db})
                    check_statistics.append({'是否存在差异': '否'})

                end_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
                check_statistics.append({'结束校验': end_time})


        except Exception as e:
            print ('数据校验失败Err: ',e)

    logging.info("Result Sendmail....")
    SendMail(conf).send_mail(error, check_statistics)
if __name__ == '__main__':
    runchecksums()
