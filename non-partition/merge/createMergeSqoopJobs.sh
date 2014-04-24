#!/bin/bash
BASEDIR=$(dirname $0)
source ${BASEDIR}/setenv.sh
sqoop_tables_file=$SQOOP_CONF_PATH

################################################################################
# Configuration Processing
################################################################################
echo "Read SQOOP configuration: $sqoop_tables_file"
index=0
while IFS=$'\t' read -r -a myArray
do
 schema[$index]=${myArray[0]}
 table[$index]=${myArray[1]}
 primary_key[$index]=${myArray[2]}
 nbr_mappers[$index]=${myArray[3]}
 nbr_reducers[$index]=${myArray[4]}
 #The primary key here is used for splitby, does not need to be unique and can be any field
 index=$(($index+1))
done < $sqoop_tables_file #File where the table names are provided


#Read the array of tables and loop through them, creating sqoop merge jobs
echo "Create MERGE SQOOP for $index tables ..."
i=0;
while [ $i -lt $index ]; do
 schema=${schema[$i]}
 table=${table[$i]}
 primaryKey=${primary_key[$i]}
 nbrReducers=${nbr_reducers[$i]}
 echo "Create merge job for $schema $table $primaryKey $nbrReducers"
 ${BASEDIR}/sqoop_merge_job_crt.sh $schema $table  $primaryKey $nbrReducers 
 #loop through entire array
 i=$(($i+1))
done
echo "Done with creating MERGE SQOOP jobs."
