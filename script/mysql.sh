# defined variable in user-defined env/hpc_mysql_credentials
# HPC_USER
# HPC_PASS
# MYSQL_USER
# MYSQL_PASS
source env/hpc_mysql_credentials

docker run --rm -it \
	--name mysql \
	-e HPC_USER=$HPC_USER \
	-e HPC_PASS=$HPC_PASS \
	-e MYSQL_USER=$MYSQL_USER \
	-e MYSQL_PASS=$MYSQL_PASS \
	-v $PWD/dockerfile/data/prepare_parquet/mysql.sh:/telecom/script/mysql.sh \
	0jacky/telecom:prepare_parquet \
	bash script/mysql.sh
