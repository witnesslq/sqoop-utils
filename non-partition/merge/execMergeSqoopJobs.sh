#!/bin/bash
BASEDIR=$(dirname $0)
sqoop_tables_file=$SQOOP_CONF_PATH

echo "Read SQOOP configuration: $sqoop_tables_file"
index=0
while IFS=$'\t' read -r -a myArray
do
 schema[$index]=${myArray[0]}
 table[$index]=${myArray[1]}
 index=$(($index+1))
done < $sqoop_tables_file #File where the table names are provided


#Read the array of tables and loop through them, performing either full/incremental sqoop depending on the user input
echo "Execute Full SQOOP for $index tables ..."
i=0;
while [ $i -lt $index ]; do
 schema=${schema[$i]}
 table=${table[$i]}
 echo "Create job for $schema $table"
 ${BASEDIR}/sqoop_merge_job_exec.sh $schema $table 
 #loop through entire array
 i=$(($i+1))
done
echo "Done with executing MERGE SQOOP jobs."
