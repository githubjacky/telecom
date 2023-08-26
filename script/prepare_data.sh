echo "setup environment variable"
source env/mysql.sh

echo "connect to gpu2 of hpc provided by BDSRC"
killall ssh
sshpass -p $HPC_PASS ssh -fN -L 3336:127.0.0.1:3306 $HPC_USER@140.112.176.245 -p 2026
mysql --user=$MYSQL_USER --password=$MYSQL_PASS --port=3336 --protocol=TCP \
	--database="telecom" <src/data/clean_data_from_raw.sql

echo "output target parquet files"
julia --project=. src/data/prepare_parquet.jl
