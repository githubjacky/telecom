# SSH Tuneling
In this tutorial, I'm going to show you how to connect to the remote server's MySQL database
from your local machine. Notice that the tutorial is for the Mac or Linux user. 


## Install MySQL On Your Local Machine
You can download the MySQL community server refering to this [website](https://www.geeksforgeeks.org/how-to-install-mysql-on-macos/).


## create ssh tunel
1. on your local machine 
```sh
ssh -fN -L 3336:127.0.0.1:3306 {bdsrc_hpc_user_name}@140.112.176.245
```
It will activate the prompt asking to input your passords in order to connect to the BDSRC
HPC. After connect to the master, let the remote MySQL database connect to your local MySQL
database through the port you just created. The local port 3336 can be customized as you
want and just remember not to use 3306. While the remote port is fixed to 3306 since MySQL 
will listen to this port as default. Plus, that's the reason why not setting the local port
to 3306. To see the meaning of the flag: f, N and L, refer to the man page.

```sh
man ssh
```

2. on your local macine
*import: whenever you log into the BDSRC master server, just leave it alone. Comback to your local machine.*
```sh
mysql --user={mysql_db_user_name} --password={mysql_db_password} --port=3336 --protocol=TCP
```
The port 3336 is just created in the previous command. If you customize one, remember to
modify. Now you can connect to the remote MySQL database!

When you don't kill the tuneling process and reconnect, warnings will be raised. Use the
following command to kill the process.
```sh
# check PID
lsof -i:{local_port} # local_port default to 3336

kill -15 {pid}
```


# Connect MySQL Database In Stata

## Install ODBC Manager: iODBC
1. build the package: [reference](https://github.com/openlink/iODBC)
    - If you are a mac user, see the README_MACOSX.md

2. modify the ~/.odbc.ini file to your need
My configuration:
```text
[ODBC Data Sources]
telecom = telecom

[telecom]
Driver = /usr/local/lib/libmyodbc8w.so
Description = BDSRC telecom database
Host = 127.0.0.1
UserName = r12323011
Password = eqatlun4e
Database = telecom
Port = 3336
```

## Install MySQL ODBC conector(driver)
- install the driver from [here](https://dev.mysql.com/downloads/connector/odbc/). You can 
decide down the package or build it from source. Check out the [manual_connector_odbc_installation](https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-installation-binary-macos.html)


## odbc
After setting up the ODBC, you just need to do the ssh tunel connecting to the remote 
MySQL database and then you can use the command `odbc` in Stata.
