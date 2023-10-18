sshpass -p $HPC_PASS \
	ssh -o StrictHostKeyChecking=no \
	-fN -L 3336:127.0.0.1:3306 \
	$HPC_USER@140.112.176.245

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

# test
# mysql --user=$MYSQL_USER --password=$MYSQL_PASS --port=3336 --protocol TCP

jupyter lab --ip=* --port=8888 --no-browser --allow-root
