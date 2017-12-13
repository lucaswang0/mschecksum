# -*- coding:utf-8 -*-
# edit by fuzongfei
# python3.6

import os
import io
import sys
import time
import argparse
import subprocess
import configparser
import smtplib
from email.mime.text import MIMEText
import mysql.connector as mdb
from mysql.connector import errorcode
from mysql.connector import constants

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

current_time = time.strftime("%Y-%m-%d", time.localtime())
checksums_log = f'/tmp/pt_table_checksum_{current_time}.log'


def get_args():
    parser = argparse.ArgumentParser(description='This a auto pt-table-checksum help document.')
    parser.add_argument('-u', '--user', type=str, help='this mysql root user.')
    parser.add_argument('-p', '--password', type=str, help='this mysql password for root user.')
    parser.add_argument('-s', '--unix_socket', type=str, help='this mysql socket.')
    parser.add_argument('-f', '--file', type=str, help='this pt-table-checksum config file.')

    args = parser.parse_args()

    config = configparser.ConfigParser(allow_no_value=True)
    config.read(args.file)
    return {'mysql': dict(config.items('mysql')), 'mail': dict(config.items('mail')), 'login_args': args}


# 定义dsns表结构
tables = {'dsns': (
    "CREATE TABLE `dsns` ("
    "  `id` INT(11) NOT NULL AUTO_INCREMENT,"
    "  `parent_id` INT(11) DEFAULT NULL,"
    "  `dsn` VARCHAR(255) NOT NULL,"
    "  PRIMARY KEY (`id`),"
    "  UNIQUE KEY `uk_dsn` (`dsn`)"
    ") ENGINE=InnoDB"
)}


def conn_mysql(kwargs):
    config = {
        'user': kwargs.user,
        'password': kwargs.password,
        # 'port': kwargs.port,
        'unix_socket': kwargs.unix_socket,
    }

    return mdb.connect(**config)


def writefile(content):
    with open(checksums_log, 'a') as file:
        file.write(content)


def removefile(filename):
    try:
        os.remove(filename)
    except FileNotFoundError as err:
        pass


_start_html = """
<html>
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <style>
        body {
            font-family: Monaco, Menlo, Consolas, "Courier New", monospace;
            font-size: 12px;
            line-height: 1.42857143;
            color: #333;
        }

        .box.box-primary {
            border-top-color: #3c8dbc;
        }

        .box {
            position: relative;
            border-radius: 3px;
            background: #ffffff;
            border-top: 3px solid #d2d6de;
            margin-bottom: 20px;
            width: 100%;
            box-shadow: 0 1px 1px rgba(0, 0, 0, 0.1);
        }
    </style>
</head>
<body>
<div class="box box-primary">
"""

_end_html = """
  </div>
  </body>
</html>
"""


class InitialDsn(object):
    def __init__(self, kwargs):
        self.db_name = kwargs['mysql']['dsndb']
        self.user = kwargs['mysql']['user']
        self.password = kwargs['mysql']['password']
        self.master = kwargs['mysql']['master']
        self.slaves = kwargs['mysql']['slave'].split(',')
        self.login_args = kwargs['login_args']

    @property
    def _init_connect(self):
        try:
            conn_mysql(self.login_args)
        except mdb.Error as err:
            print(f'-> 无法连接到数据库, 错误：{err.msg}')
            print('-> 程序退出')
            sys.exit(1)
        else:
            conn = conn_mysql(self.login_args)
            return conn

    @property
    def initial_dsn(self):
        conn = self._init_connect
        cursor = conn.cursor(constants.RefreshOption.GRANT)

        # 创建库
        try:
            cursor.execute(f"CREATE DATABASE {self.db_name} DEFAULT CHARACTER SET 'utf8'")
        except mdb.Error as err:
            if err.errno == errorcode.ER_DB_CREATE_EXISTS:
                print(f'-> 库：{self.db_name} 已存在，跳过')
            else:
                print(err.msg)
                exit(1)
        else:
            print(f'-> 库：{self.db_name} 创建成功')

        # 创建表
        conn.database = self.db_name
        for name, ddl in tables.items():
            try:
                cursor.execute(ddl)
                cursor.reset()
            except mdb.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print(f'-> 表：{name} 已存在，跳过')
                    cursor.execute(f'delete from {name}')
                    cursor.reset()
                    print(f'--> 清空表: {name}')
            else:
                print(f'-> 表：{name} 创建成功')

        # 插入被检测的从库的信息到表dsns
        for i in self.slaves:
            slave = i.split(':')[0]
            slave_port = i.split(':')[1]
            add_slave = f"insert into dsns(dsn) values('h={slave},u={self.user},p={self.password},P={slave_port}')"
            cursor.execute(add_slave)
            cursor.reset()
            print(f'-> 插入dsn：h={slave},u={self.user},p={self.password},P={slave_port}')

        # 创建检测用户
        try:
            add_user = f"create user {self.user}@'{self.master}' identified by '{self.password}'"
            cursor.execute(add_user)
            cursor.reset()
            print(f"-> 用户：{self.user}@'{self.master}' 创建成功")
        except mdb.Error as err:
            if err.errno == errorcode.ER_CANNOT_USER:
                print(f"-> 用户：{self.user}@'{self.master}' 已存在, 跳过")
        else:
            add_grant = f"grant usage,select,insert,update,delete,create,process,super,replication slave on *.* to {self.user}@'{self.master}'"
            cursor.execute(add_grant)
            cursor.reset()
            print(
                f"-> 授权，用户：{self.user}@'{self.master}' 权限：usage,select,insert,update,delete,create,process,super,replication slave on *.*")

        conn.commit()
        cursor.close()
        conn.close()


class checksums(object):
    def __init__(self, parameter):
        self.user = parameter['user']
        self.password = parameter['password']
        self.host = parameter['host']
        self.port = parameter['master'].split(':')[1]
        self.db = parameter['database']
        self.dsndb = parameter['dsndb']
        self.pt_table_checksum = parameter['pt_table_checksum']
        self.args = f"--recursion-method dsn=D={self.dsndb},t=dsns " \
                    f"--replicate {self.dsndb}.checksums -d {self.db} " \
                    f"--nocheck-replication-filters --no-check-binlog-format --empty-replicate-table"

        self.cmd = ' '.join(
            (self.pt_table_checksum, f"-u{self.user} -p'{self.password}' -h{self.host} -P{self.port}", self.args))

    @property
    def check(self):
        print(f'-> 执行数据主从校验')
        print(f'-->执行命令输出：\n{self.cmd}')
        status, output = subprocess.getstatusoutput(self.cmd)
        result = {'status': status, 'output': output}
        return result

    @property
    def diff(self):
        """
        ：使用上一次数据校验存储在--replicate指定的校验信息表的校验数据，来检测主从数据的一致性
        : 只输出数据不一致的表的校验结果
        """
        diff_cmd = ' '.join((self.cmd, '--replicate-check-only'))
        print(f'-> 执行主从数据差异检测')
        print(f'-->执行命令输出：\n{diff_cmd}')
        status, output = subprocess.getstatusoutput(diff_cmd)
        return output


def send_mail(parameter, type=0):
    mail_sender = parameter['mail_sender']
    mail_receiver = parameter['mail_receiver'].split(',')
    mail_host = parameter['mail_host']
    mail_user = parameter['mail_user']
    mail_pass = parameter['mail_pass']

    message = ''.join(open(checksums_log, 'r').readlines())

    msg = MIMEText(message, _subtype='html', _charset='gb2312')
    msg['Subject'] = '[{status}]_{title}'.format(status='OK' if type == 0 else 'WARN', title=parameter['title'])
    msg['From'] = mail_sender
    msg['To'] = ";".join(mail_receiver)

    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.ehlo()
        server.starttls()
        server.login(mail_user, mail_pass)
        server.sendmail(mail_sender, mail_receiver, msg.as_string())
        server.close()
    except Exception as err:
        print(err.msg)


def get_check_info(parameter):
    info = f"主库：{parameter['master']}<br>" \
           f"从库：{parameter['slave']}<br>" \
           f"被检查的库：{parameter['database']}<br>" \
           f"检测时间：{current_time}<br>"
    writefile(info)


def run_check():
    arguments = get_args()
    InitialDsn(arguments).initial_dsn
    removefile(checksums_log)

    writefile(_start_html)
    get_check_info(arguments['mysql'])

    # 开始检测
    checksum = checksums(arguments['mysql'])
    check_result = checksum.check
    if check_result['status'] in [0, 16, 32, 64]:
        diff_output = checksum.diff
        type = 0

        # 写入差异数据的检测结果到日志文件
        if diff_output:
            writefile("<div style='color:red'>")
            writefile('<br>')
            writefile('检测出主从数据不一致[pt-table-checksum]：')
            writefile('<br>'.join([x.replace(' ', '&nbsp;') for x in diff_output.split('\n')]))
            writefile("</div>")
            type = 1

        # 将检测输出保存到日志文件
        writefile("<div style='color:red'>")
        writefile('<br>')
        writefile('检测过程输出[pt-table-checksum]：')
        writefile("</div>")
        writefile("<div>")
        writefile('<br>'.join([x.replace(' ', '&nbsp;') for x in check_result['output'].split('\n')]))
        writefile("</div>")
        writefile(_end_html)
        send_mail(arguments['mail'], type)
    else:
        # 如果异常，将异常信息写入到日志文件
        writefile(check_result['output'])
        writefile(_end_html)
        send_mail(arguments['mail'], type=1)


if __name__ == '__main__':
    run_check()
