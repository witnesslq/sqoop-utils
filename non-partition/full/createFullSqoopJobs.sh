#!/bin/bash
if [ $# -ne 1 ] ; then
   echo "USAGE: $0 [timestamp:yyyy-MM-dd-HH.mm.ss.ffffff]"
   exit
fi

BASEDIR=$(dirname $0)
source ${BASEDIR}/setenv.sh
sqoop_tables_file=$SQOOP_CONF_PATH
time_stamp=$1

echo "Read SQOOP configuration: $sqoop_tables_file"
index=0
while IFS=$'\t' read -r -a myArray
do
 schema[$index]=${myArray[0]}
 table[$index]=${myArray[1]}
 primary_key[$index]=${myArray[2]}
 #The primary key here is used for splitby, does not need to be unique and can be any field
 index=$(($index+1))
done < $sqoop_tables_file #File where the table names are provided


#Read the array of tables and loop through them, performing either full/incremental sqoop depending on the user input
echo "Create Full SQOOP for $index tables ..."
i=0;
while [ $i -lt $index ]; do
 schema=${schema[$i]}
 table=${table[$i]}
 primary_key=${primary_key[$i]}
 tablename="$schema.$table"
 echo "Create job for $schema $table $primary_key $time_stamp"
 ${BASEDIR}/sqoop_full_job_crt.sh $schema $table $primary_key $time_stamp
 #loop through entire array
 i=$(($i+1))
done
echo "Done with creating FULL SQOOP jobs."
