sshpass -p $HPC_PASS \
	ssh -fN -L 3336:127.0.0.1:3306 $HPC_USER@140.112.176.245 -p 2026

service mariadb start

cat >>/root/.odbc.ini <<EOF
[telecom]
Driver = mysql_driver
Description = telecom database in the master branch of the BDSRC HPC
Server = 127.0.0.1
Port = 3336
Database = telecom
User = $MYSQL_USER
Password = $MYSQL_PASS
EOF

mysql -u $MYSQL_USER -p $MYSQL_PASS telecom <clean_data_from_raw.sql
