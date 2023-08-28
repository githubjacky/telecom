# defined variable in user-defined env/hpc_mysql_credentials
# HPC_USER
# HPC_PASS
# MYSQL_USER
# MYSQL_PASS
source env/hpc_mysql_credentials

docker run --rm \
	--name prepare_parquet \
	-e HPC_USER=$HPC_USER \
	-e HPC_PASS=$HPC_PASS \
	-e MYSQL_USER=$MYSQL_USER \
	-e MYSQL_PASS=$MYSQL_PASS \
	-v $PWD/src/data/clean_data_from_raw.sql:/telecom/clean_data_from_raw.sql \
	-v $PWD/dockerfile/data/prepare_parquet/prepare_parquet.sh:/telecom/script/prepare_parquet.sh \
	0jacky/telecom:prepare_parquet \
	bash script/prepare_parquet.sh
