# SSH Tuneling
*Notice that the tutorial is for the Mac or Linux user.*

In this tutorial, I will demonstrate how to connect to a remote server's MySQL database 
from your local machine or any other machine which the database isn't stored on.


## Install MySQL On Your Local Machine
You can download the MySQL community server refering to this [website](https://www.geeksforgeeks.org/how-to-install-mysql-on-macos/).


## create ssh tunel
1. on your local machine 
```sh
ssh -fN -L 3336:127.0.0.1:3306 {bdsrc_hpc_user_name}@140.112.176.245
```
It will activate the prompt asking to input your passords in order to connect to the BDSRC
HPC. After connect to the master, let the remote MySQL database connect to your local MySQL
database through the port you just created. The local port 3336 can be customized and 
just remember not to use 3306. While the remote port is fixed to 3306 since MySQL 
will listen to this port by default. Plus, that's the reason why not setting the local port
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
Follow the instruction [here](https://gist.github.com/erikvw/f178b6a2b66383d83b98f916f9b5699b)
1. install ODBC mananger
```sh
brew install unixodbc
```

2. install ODBC MySQL driver(connector)
*weird: use MariaDB connector work*
```sh
brew install mariadb-connector-odbc
```

3. driver cofiguration
- check the path of the configuration file
```sh
odbcinst -j
```

- create the configuration file(.odbc.ini) in your home directory
```sh
[! -f $HOME/.odbc.ini] && touch .odbc.ini
```

- input the following parameters in the file
    - You have to check out the version of the driver. In may case, it's 3.1.19
    - the port is customized for the remote MySQL server to listen
    - the customized port is 3336 in the above section introducing ssh tuneling
```sh
[{your_database_name}]
Driver      = /opt/homebrew/Cellar/mariadb-connector-odbc/3.1.19/lib/mariadb/libmaodbc.dylib
Description = {description of your database}
Server      = 127.0.0.1
Port        = {your_customized_port}
Database    = {your_database_name}
User        = {your_database_username}
Password    = {your_database_password}
```

4. ssh tunel to remote MySQL server

5. test remote odbc connection
```
isql -v {your_database_name}
```

6. configure the `obsd` command in Stata 16.
```sh
export DYLD_LIBRARY_PATH=/opt/homebrew/lib/ && \
/Applications/Stata/StataSE.app/Contents/MacOS/stata-se 
```

- stata command:
    - `set odbcmgr unixodbc[, permanently]`
    - parameter: permanently is optional
