介绍
============
安装pt-tools

安装python3.6 <br>

安装mysql-connector-python-2.1.6 <br>

cd mysql-connector-python-2.1.6 && python3 setup.py install <br>

检测方式

使用dsn方式检测从

功能
	1. 自动创建检测用户、并授权
	2. 自动生成pt-table-checksum命令，并校验
	3. 将校验后的结果通过邮件发送给指定的收件人
	
执行

python3 ms_checksum.py -uroot -p'123.com' -s /usr/local/mysql/mysql.sock -f ms_checksum.cnf

输出：
-> 库：percona 创建成功
-> 表：dsns 创建成功
-> 插入dsn：h=10.72.63.199,u=checksums,p=123.com,P=3306
-> 插入dsn：h=10.72.63.204,u=checksums,p=123.com,P=3306
-> 用户：checksums@'10.72.63.197' 创建成功
-> 授权，用户：checksums@'10.72.63.197' 权限：usage,select,insert,update,delete,create,process,super,replication slave on *.*
-> 执行数据主从校验
-->执行命令输出：
/usr/bin/pt-table-checksum -uchecksums -p'123.com' -h10.72.63.197 -P3306 --recursion-method dsn=D=percona,t=dsns --replicate percona.checksums -d user_center,ttt --nocheck-replication-filters --no-check-binlog-format --empty-replicate-table
-> 执行主从数据差异检测
-->执行命令输出：
/usr/bin/pt-table-checksum -uchecksums -p'123.com' -h10.72.63.197 -P3306 --recursion-method dsn=D=percona,t=dsns --replicate percona.checksums -d user_center,ttt --nocheck-replication-filters --no-check-binlog-format --empty-replicate-table --replicate-check-only

接收到的邮件输出

主库：10.72.63.197:3306
从库：10.72.63.199:3306,10.72.63.204:3306
被检查的库：user_center,ttt
检测时间：2017-12-13

检测出主从数据不一致[pt-table-checksum]：Differences on mysql-test-199
TABLE CHUNK CNT_DIFF CRC_DIFF CHUNK_INDEX LOWER_BOUNDARY UPPER_BOUNDARY
ttt.t1 1 2 1   

Differences on mysql-test-204
TABLE CHUNK CNT_DIFF CRC_DIFF CHUNK_INDEX LOWER_BOUNDARY UPPER_BOUNDARY
ttt.t1 1 1 1   

检测过程输出[pt-table-checksum]：
            TS ERRORS  DIFFS     ROWS  CHUNKS SKIPPED    TIME TABLE
12-13T12:25:21      0      1        3       1       0   0.021 ttt.t1
12-13T12:25:21      0      0        5       1       0   0.018 user_center.admin_card_type
12-13T12:25:21      0      0       11       1       0   0.018 user_center.admin_menu
12-13T12:25:21      0      0      264       1       0   0.019 user_center.admin_operation_log
12-13T12:25:21      0      0        0       1       0   0.018 user_center.admin_permissions
12-13T12:25:21      0      0        3       1       0   0.018 user_center.admin_role_menu
12-13T12:25:22      0      0        0       1       0   0.275 user_center.admin_role_permissions
12-13T12:25:22      0      0        1       1       0   0.023 user_center.admin_role_users
12-13T12:25:22      0      0        1       1       0   0.018 user_center.admin_roles

定时任务
00 01 */1 * * python3 ms_checksum.py -uroot -p'123.com' -s /usr/local/mysql/mysql.sock -f ms_checksum.cnf
