#!/bin/bash
if [ $# -ne 5 ] ; then
   echo "USAGE: $0 [schema] [tablename] [split-by] [number-mappers] [timestamp:yyyy-MM-dd-HH.mm.ss.ffffff]"
   exit
fi

schema=`echo $1 | tr '[a-z]' '[A-Z]'`
table=`echo $2 | tr '[a-z]' '[A-Z]'`
split_by=`echo $3 | tr '[a-z]' '[A-Z]'`
nbr_mappers=$4
lastval=$5

# These variables should be set by sourcing setenv.sh
jdbc_url=$SQOOP_JDBC_URL
output_path=$SQOOP_OUTPUT_PATH
backup_path=$SQOOP_BACKUP_PATH
log_path=$SQOOP_LOG_PATH
jdbc_pswd=$SQOOP_PASS_WORD


# These variables should be changed for specific use
# Make sure to not have a trailing / after the path
# ----------------------------------------------------------------------------
conn=${jdbc_url}":currentSchema=$schema;"

now_date=`date +"%Y-%m-%d"`
now_time=`date +"%H.%M.%S"`
user=`whoami | tr '[a-z]' '[A-Z]'`

tablename="$schema.$table"
jobname="INCR.$schema.$table"
output_dir=$output_path/$schema.$table
sqoop_incr_job_creation_log=$log_path/SQOOP_INCR_JOB_CREATION_${schema}-${table}_${now_date}_${now_time}.log


echo "-------------------------------------------------------------------------------------------
 		        	INCREMENTAL SQOOP JOB CREATION
-------------------------------------------------------------------------------------------
INFO Script $0
INFO Table $tablename
INFO Number_Of_Mappers $nbr_mappers
INFO Source $conn
INFO Target $output_dir" > $sqoop_incr_job_creation_log

################################################################################
# Check if user has permissions 
################################################################################
if [ $user != $SQOOP_USER ] ; then
   echo "WARN $user tried to run this script" >> $sqoop_incr_job_creation_log
   echo "You do not have permission to run this script. Please use userid hdobudw"
   exit
fi

################################################################################
# Check if $table.java  already exists
################################################################################
if [ -e $table.java ] ; then
   echo "WARN Table temp file exists.. Removing it"
   rm -f $table.java
fi

################################################################################
# Sqoop incremental job creation 
################################################################################
# Use --verbose to see a detailed log of sqoop import command

# Delete job if already exists
sqoop job \
	--delete $jobname
# Can be customized
import_sql="select t.* from $tablename as t where \$CONDITIONS"
# Create job
sqoop job -Dpool.name=sqoop \
	-Dmapred.job.name=$jobname \
	--create $jobname \
	-- import \
	--connect $conn \
	--username $user \
	--password $jdbc_pswd \
	--num-mappers $nbr_mappers \
	--compress \
	--compression-codec org.apache.hadoop.io.compress.BZip2Codec \
	--query "${import_sql}" \
	--check-column LAST_MOD_TS \
	--incremental lastmodified \
	--last-value $lastval \
	--target-dir $output_dir \
	--split-by $split_by \
	--hive-drop-import-delims \
	--null-string '' --null-non-string '' \
	--fields-terminated-by '\0x001' \
        --lines-terminated-by '\n'

echo "INFO Incremental Sqoop Job $jobname created 
-------------------------------------------------------------------------------------------" >> $sqoop_incr_job_creation_log

# Remove temp files from current directory
rm -f ./$table.*

cat $sqoop_incr_job_creation_log
