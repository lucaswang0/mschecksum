# -*- coding:utf-8 -*-
# edit by fuzongfei
# python3.6

import argparse
import configparser
import logging
import socket
import smtplib
import subprocess
import sys
import time
from email.mime.text import MIMEText
from os.path import isfile

import mysql.connector as mdb
from mysql.connector import constants
from mysql.connector import errorcode

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)

logger = logging.getLogger(__name__)

hostname = socket.gethostname()
current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
checksums_log = f'/tmp/pt_table_checksum_{current_time}.log'

check_statistics = [{'主机名': hostname}]

def get_arguments():
    parser = argparse.ArgumentParser(description='This a auto pt-table-checksum help document.')
    parser.add_argument('-u', '--user', type=str, help='this mysql root user.')
    parser.add_argument('-p', '--password', type=str, help='this mysql password for root user.')
    parser.add_argument('-s', '--unix_socket', type=str, help='this mysql socket.')
    parser.add_argument('-f', '--file', type=str, help='this pt-table-checksum config file.')

    args = parser.parse_args()
    return args


class ConnMysql(object):
    def __init__(self, config, commit=False):
        self.config = config
        self.conn = None
        self.cursor = None
        self.commit = commit

    def __enter__(self):
        if self.conn is not None:
            raise RuntimeError('Already connection')
        try:
            self.conn = mdb.connect(**self.config)
        except mdb.Error as err:
            logger.error(err.msg)
            sys.exit(1)
        self.conn.set_charset_collation('utf8')
        self.cursor = self.conn.cursor(constants.RefreshOption.GRANT)
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.commit is True:
            self.conn.commit()
        self.cursor.close()
        self.conn.close()
        self.cursor = None
        self.conn = None


class General(object):
    def __init__(self, file):
        if isfile(file):
            config = configparser.ConfigParser(allow_no_value=True)
            config.read(file)

            Mysql = config['mysql']
            self.user = Mysql['user']
            self.host = Mysql['host']
            self.password = Mysql['password']
            self.database = Mysql['database']
            self.master = Mysql['master'].split(':')[0]
            self.master_port = Mysql['master'].split(':')[1]
            self.slave = Mysql['slave'].split(',')
            self.dsndb = Mysql['dsndb']
            self.pt_table_checksum = Mysql['pt_table_checksum']

            Mail = config['mail']
            self.title = Mail['title']
            self.mail_sender = Mail['mail_sender']
            self.mail_receiver = Mail['mail_receiver']
            self.mail_host = Mail['mail_host']
            self.mail_user = Mail['mail_user']
            self.mail_pass = Mail['mail_pass']

class CreateSchema(General):
    def __init__(self, file, config_file):
        self.file = file
        self.config_file = config_file
        General.__init__(self, self.file)
        self.cnx = ConnMysql(self.config_file, commit=True)

        self.tables = {'dsns': (
            f"CREATE TABLE {self.dsndb}.`dsns` ("
            "  `id` INT(11) NOT NULL AUTO_INCREMENT,"
            "  `parent_id` INT(11) DEFAULT NULL,"
            "  `dsn` VARCHAR(255) NOT NULL,"
            "  PRIMARY KEY (`id`),"
            "  UNIQUE KEY `uk_dsn` (`dsn`)"
            ") ENGINE=InnoDB"
        )}

    def createschema(self):
        with self.cnx as cursor:
            try:
                create_schema = f"CREATE DATABASE {self.dsndb} DEFAULT CHARACTER SET 'utf8'"
                cursor.execute(create_schema)
            except mdb.Error as err:
                if err.errno == errorcode.ER_DB_CREATE_EXISTS:
                    logger.warning(f"database {self.dsndb} already exists, skip...")
                else:
                    logger.error(err.msg + ', skip...')
            else:
                logger.info(f"database {self.dsndb} create success")

    def createtable(self):
        with self.cnx as cursor:
            for name, ddl in self.tables.items():
                try:
                    cursor.execute(ddl)
                    logger.info(f"table {name} create success")
                except mdb.Error as err:
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        logger.warning(f"table {name} already exists, skip...")
                        cursor.execute(f'delete from {self.dsndb}.{name}')
                        cursor.reset()
                        logger.info(f"clear the table {self.dsndb}.{name}")
                        continue

    def insertrecord(self):
        with self.cnx as cursor:
            for i in self.slave:
                slave = i.split(':')[0]
                slave_port = i.split(':')[1]
                add_slave = f"insert into {self.dsndb}.dsns(dsn) values('h={slave},u={self.user},p={self.password},P={slave_port}')"
                cursor.execute(add_slave)
                logger.info(f"insert dsn record: h={slave},u={self.user},p={self.password},P={slave_port}")

    def createuser(self):
        with self.cnx as cursor:
            add_user = f"create user {self.user}@'{self.master}' identified by '{self.password}'"
            try:
                cursor.execute(add_user)
            except mdb.Error as err:
                if err.errno == errorcode.ER_CANNOT_USER:
                    logger.warning(f"user {self.user}@'{self.master}' already exists, skip...")
            else:
                add_grant = f"grant usage,select,insert,update,delete,create,process,super,replication slave on *.* to {self.user}@'{self.master}'"
                cursor.execute(add_grant)
                logger.info(
                    f"user: {self.user}@'{self.master}'\n\tgrant privileges: usage,select,insert,update,delete,create,process,super,replication slave on *.*")

    def run(self):
        self.createschema()
        self.createtable()
        self.insertrecord()
        self.createuser()


class CheckSums(General):
    def __init__(self, file):
        self.file = file
        General.__init__(self, self.file)

        self.args_options = f"--recursion-method dsn=D={self.dsndb},t=dsns " \
                            f"--replicate {self.dsndb}.checksums -d {self.database} " \
                            f"--nocheck-replication-filters --no-check-binlog-format --empty-replicate-table"

        self.cmd = ' '.join(
            (self.pt_table_checksum, f"-u{self.user} -p'{self.password}' -h{self.host} -P{self.master_port}",
             self.args_options))

        check_statistics.append({
            '主库': self.master,
            '从库': ','.join(self.slave),
            '被检查的库': self.database,
        })

    def check(self):
        logger.info("begin to perform data valid check")
        logger.info(f"ouput command: \n\t{self.cmd}")
        status, output = subprocess.getstatusoutput(self.cmd)
        return {'status': status, 'output': output}

    def diff(self):
        """
        ：使用上一次数据校验存储在--replicate指定的校验信息表的校验数据，来检测主从数据的一致性
        : 只输出数据不一致的表的校验结果
        """
        diff_cmd = ' '.join((self.cmd, '--replicate-check-only'))
        logger.info("begin to perform data difference detection")
        logger.info(f"ouput command: \n\t{diff_cmd}")
        status, output = subprocess.getstatusoutput(diff_cmd)
        return output


class SendMail(General):
    def __init__(self, file):
        self.file = file
        General.__init__(self, self.file)

    def send_mail(self, type, data):
        FMT_INFO = '{} --> {}\n'
        content_list = []
        for i in data:
            for k in i:
                content_list.append(FMT_INFO.format(k, i[k]))
        message = '\n'.join(content_list)

        msg = MIMEText(message, _subtype='plain', _charset='gb2312')
        msg['Subject'] = '[{status}]_{title}'.format(status='OK' if type == 0 else 'WARN', title=self.title)
        msg['From'] = self.mail_sender
        msg['To'] = ";".join(list(self.mail_receiver.split(',')))

        try:
            server = smtplib.SMTP()
            server.connect(self.mail_host)
            server.ehlo()
            server.starttls()
            server.login(self.mail_user, self.mail_pass)
            server.sendmail(self.mail_sender, self.mail_receiver, msg.as_string())
            server.close()
        except Exception as err:
            logger.error(err.msg)


def run_checksums():
    arguments = get_arguments()
    file = arguments.file
    config_file = {
        'user': arguments.user,
        'password': arguments.password,
        'unix_socket': arguments.unix_socket,
    }

    CreateSchema(file, config_file).run()

    checksum = CheckSums(file)
    check = checksum.check()
    if check['status'] in [0, 16, 32, 64]:
        type = 0
        diff = checksum.diff()
        if diff:
            type = 1
            check_statistics.append({'是否存在差异': '是', '差异输出': '\n'+diff})
            SendMail(file).send_mail(type, check_statistics)
        else:
            check_statistics.append({'是否存在差异': '否'})
            SendMail(file).send_mail(type, check_statistics)

if __name__ == '__main__':
    run_checksums()
