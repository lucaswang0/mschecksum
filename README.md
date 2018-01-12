介绍
============
#安装pt-tools <br>
#安装python3.6 <br>
#安装mysql-connector-python-2.1.6 <br>
#cd mysql-connector-python-2.1.6 && python3 setup.py install <br>

**检测方式** <br>
使用dsn方式检测从 <br>

**功能**<br>
1.自动创建检测用户、并授权
2.自动生成pt-table-checksum命令，并校验
3.将校验后的结果通过邮件发送给指定的收件人
	
**执行**<br>
python3 ms_checksum.py -uroot -p'123.com' -s /usr/local/mysql/mysql.sock -f ms_checksum.cnf <br>

输出：<br>
-> 库：percona 创建成功 <br>
-> 表：dsns 创建成功 <br>
-> 插入dsn：h=10.72.63.199,u=checksums,p=123.com,P=3306 <br>
-> 插入dsn：h=10.72.63.204,u=checksums,p=123.com,P=3306 <br> 
-> 用户：checksums@'10.72.63.197' 创建成功 <br>
-> 授权，用户：checksums@'10.72.63.197' 权限：usage,select,insert,update,delete,create,process,super,replication slave on *.* <br>

-> 执行数据主从校验 <br>
-->执行命令输出：<br>
/usr/bin/pt-table-checksum -uchecksums -p'123.com' -h10.72.63.197 -P3306 --recursion-method dsn=D=percona,t=dsns --replicate percona.checksums -d user_center,ttt --nocheck-replication-filters --no-check-binlog-format --empty-replicate-table <br>

-> 执行主从数据差异检测 <br>
-->执行命令输出：<br>
/usr/bin/pt-table-checksum -uchecksums -p'123.com' -h10.72.63.197 -P3306 --recursion-method dsn=D=percona,t=dsns --replicate percona.checksums -d user_center,ttt --nocheck-replication-filters --no-check-binlog-format --empty-replicate-table --replicate-check-only <br>


**接收到的邮件输出**

主库：10.72.63.197:3306 <br>
从库：10.72.63.199:3306,10.72.63.204:3306 <br>
被检查的库：user_center,ttt <br>
检测时间：2017-12-13 <br>

检测出主从数据不一致[pt-table-checksum]：Differences on mysql-test-199 <br>
TABLE CHUNK CNT_DIFF CRC_DIFF CHUNK_INDEX LOWER_BOUNDARY UPPER_BOUNDARY <br>
ttt.t1 1 2 1 <br>

Differences on mysql-test-204 <br>
TABLE CHUNK CNT_DIFF CRC_DIFF CHUNK_INDEX LOWER_BOUNDARY UPPER_BOUNDARY <br>
ttt.t1 1 1 1 <br>


**定时任务**

00 01 */1 * * python3 ms_checksum.py -uroot -p'123.com' -s /usr/local/mysql/mysql.sock -f ms_checksum.cnf
