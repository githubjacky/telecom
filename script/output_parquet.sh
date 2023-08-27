# defined variable in user-defined env/hpc_mysql_credentials
# HPC_USER
# HPC_PASS
# MYSQL_USER
# MYSQL_PASS
source env/hpc_mysql_credentials

docker run --rm -it \
	-e HPC_USER=$HPC_USER \
	-e HPC_PASS=$HPC_PASS \
	-e MYSQL_USER=$MYSQL_USER \
	-e MYSQL_PASS=$MYSQL_PASS \
	-v $PWD/src/data/output_parquet.jl:/telecom/output_parquet.jl \
	-v $PWD/dockerfile/data/output_parquet/output_parquet.sh:/telecom/script/output_parquet.sh \
	-v /telecom/output/processed:$PWD/data/processed \
	0jacky/telecom:output_parquet \
	bash script/output_parquet.sh
