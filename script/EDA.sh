# defined variable in user-defined env/hpc_mysql_credentials
# HPC_USER
# HPC_PASS
# MYSQL_USER
# MYSQL_PASS
source env/hpc_mysql_credentials

docker run --rm \
	-e HPC_USER=$HPC_USER \
	-e HPC_PASS=$HPC_PASS \
	-e MYSQL_USER=$MYSQL_USER \
	-e MYSQL_PASS=$MYSQL_PASS \
	-v $PWD/notebooks:/telecom/notebooks \
	-v $PWD/dockerfile/data/prepare_parquet/EDA.sh:/telecom/script/EDA.sh \
	0jacky/telecom:prepare_parquet \
	bash script/data_service.sh
