#!/usr/bin/zsh

# MySQL credentials and configurations
MYSQL_USER="r12323011"
MYSQL_PASS="altERIhk#k34"
MYSQL_DB="telecom"
CSV_DIR="/home/mysql_output/node05"
# FILE_PREFIX="serv_acct_item_0838_"
FILE_PREFIX="TB_ASZ_CDMA_0838_"
# TABLE_PREFIX="serv_acct_item_0838_"
TABLE_PREFIX="tb_asz_cdma_0838_"

# Loop through the specified date range
for year in {2013..2014}; do
	for month in {01..12}; do
		# Skip irrelevant dates
		# if [ $year -eq 2013 -a $month -lt 07 ] || [ $year -eq 2014 -a $month -gt 06 ]; then
		#           continue
		#       fi
        if [ $year -eq 2014 ]; then
            continue
        fi
		
		if [ $year -eq 2013 -a $month -lt 07 ] || [ $year -eq 2013 -a $month -gt 11 ]; then
			continue
		fi

		# Construct the file name and table name
		DATE="$year$month"
        # FILE_SUFFIX="_out.csv"
        FILE_SUFFIX="_NEW_M1_out.csv"
		FILE_NAME="${FILE_PREFIX}${DATE}${FILE_SUFFIX}"
		TABLE_NAME="${TABLE_PREFIX}${DATE}"

		# Check if file exists
		if [ -f "$CSV_DIR/$FILE_NAME" ]; then
			# Construct the MySQL command
			SQL="LOAD DATA LOCAL INFILE '$CSV_DIR/$FILE_NAME'
                 INTO TABLE $TABLE_NAME
                 FIELDS TERMINATED BY ','
                 OPTIONALLY ENCLOSED BY '\"'
                 LINES TERMINATED BY '\n'
                 IGNORE 1 LINES;"

			# Execute the command
			mysql --local-infile=1 -u $MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "$SQL"
			echo "Imported $FILE_NAME into $TABLE_NAME"
		else
			echo "File $FILE_NAME does not exist, skipping."
		fi
	done
done

echo "All relevant files have been imported."
