#/bin/sh

HPC_USER=$1
HPC_PASS=$2
MYSQL_USER=$3
MYSQL_PASS=$4

docker run --rm \
	-e HPC_USER=$HPC_USER \
	-e HPC_PASS=$HPC_PASS \
	-e MYSQL_USER=$MYSQL_USER \
	-e MYSQL_PASS=$MYSQL_PASS \
	-p 8888:8888 \
	-v $PWD/notebooks:/telecom/notebooks \
	-v $PWD/docker/script/data_service.sh:/telecom/script/data_service.sh \
	0jacky/telecom:data_servicee \
	bash script/data_service.sh
