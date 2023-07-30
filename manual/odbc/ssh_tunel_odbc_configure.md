# SSH Tuneling
*The tutorial is for the Mac or Linux user. As a mac user, I use 
homebrew as my package manager. For linux user you should be able to find the 
right package and download it from your own package manager.*

In the first part of the tutorial, I will demonstrate how to connect to a remote 
server's MySQL database from your local machine or any other machine which the 
database isn't stored on. The concept is to let the remote database server 
listen to the local port you open.
In the second part, I will show you how to setup the 
ODBC(Open Database Connectivity) allowing prgramming language such as Python,
Julia to access the remote database server.
Finaly, conclude with the workflow.


## Install MySQL On Your Local Machine
You can download the MySQL community server refering to this [website](https://www.geeksforgeeks.org/how-to-install-mysql-on-macos/).
Notice that if you don't want to manipulate the database through sql on your
machine, there is no need to install the MySQL server.


## create ssh tunel
1. on your local machine 
- server IP of the BDSRC: 140.112.176.245
```sh
ssh -fN -L 3336:127.0.0.1:3306 {bdsrc_hpc_user_name}@{your_server_IP}
```
It will activate the prompt asking to input your passords in order to connect 
to the BDSRC HPC. After connect to the master branch of the HPC, let the 
remote MySQL database connect to your local MySQL database through the port 
you just created which is 3336. The local port 3336 can be customized and 
just remember not to use 3306. While the remote port is fixed to 3306 since MySQL 
will listen to this port by default. Plus, that's the reason why not setting the 
local port to 3306. To see the meaning of the flag: f, N and L, refer to the man page.

```sh
man ssh
```

2. on your local macine
```sh
mysql --user={mysql_db_user_name} --password={mysql_db_password} --port=3336 --protocol=TCP
```
The port 3336 is just created in the previous command. If you customize one, 
remember to modify. Now you can manipulate MySQL database!

When you don't kill the tuneling process and reconnect, warnings will be raised. Use the
following command to kill the process.
```sh
# check PID
lsof -i:{local_port} # local_port default to 3336

kill -15 {pid}
```


# ODBC configuration 
1. install ODBC mananger
```sh
brew install unixodbc
```
If you are a mac user, there will the chance suggesting you to use iODBC. However,
I personally think unixodbc is far way more convenient and easy to configure 
thanks to the uitility `odbcinest` and `isql`.


2. install ODBC MySQL driver(connector)
```sh
brew install mariadb-connector-odbc
```
*weird: use MariaDB connector will work out of box*

3. driver cofiguration
- check the path of the configuration file
```sh
odbcinst -j
```
Remember to check wheter you place the configuration file such as odbinst.ini and
odbc.ini in the right path. Whenever you encounter the error, ensure your 
system can recogonize the settings first.

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
```sh
ssh -fN -L 3336:127.0.0.1:3306 {bdsrc_hpc_user_name}@{your_server_IP}
```

5. test remote odbc connection
```
isql -v {your_database_name}
```
Congrad if this command don't create any error.

To see how to utilize the ODBC API with programming language, refer to 
odbc_{programming_language}.qmd.


## Workflow
1. create a ssh tunel with your remote database server, making it listen to your
local port. 
2. connect your application(programming language) with database through ODBC.
