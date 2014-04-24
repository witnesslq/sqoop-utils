#!/bin/bash
if [ $# -ne 4 ] ; then
   echo "USAGE: $0 [schema] [tablename] [split-by] [timestamp]"
   exit
fi

schema=`echo $1 | tr '[a-z]' '[A-Z]'`
table=`echo $2 | tr '[a-z]' '[A-Z]'`
split_by=`echo $3 | tr '[a-z]' '[A-Z]'`
sqoop_until=$4
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
jobname="FULL.$schema.$table"
output_dir=$output_path/$schema.$table
sqoop_job_creation_log=$log_path/SQOOP_FULL_JOB_CREATION_${schema}-${table}_${now_date}_${now_time}.log

echo "-------------------------------------------------------------------------------------------
 			          FULL SQOOP JOB CREATION 
-------------------------------------------------------------------------------------------
INFO SCRIPT $0
INFO Table $tablename
INFO Source $conn
INFO Target $output_dir" > $sqoop_job_creation_log

################################################################################
# Check if user has permissions 
################################################################################
if [ $user != $SQOOP_USER ] ; then
   echo "WARN $user tried to run this script" >> $sqoop_job_creation_log
   echo "You do not have permission to run this script. Please use userid hdobudw"
   exit 1
fi

################################################################################
# Check if output directory already exists
################################################################################
#if hadoop fs -test -d $output_dir; then
#   echo "WARN Output directory already exists, moving it to backup directory" >> $sqoop_full_job_creation_log
#   hadoop fs -mv $output_path/$schema.$table $backup_path/${schema}.${table}_${now_date}_${now_time}
#fi

if [ -e $table.java ] ; then
   echo "WARN Table temp file exists.. Removing it"
   rm -f $table.java 
fi

################################################################################
# Sqoop full job creation
################################################################################
# Use --verbose to see a detailed log of sqoop import command

# Delete job if already exists
sqoop job \
	--show $jobname \
	2>  /dev/null > /dev/null
if [ $? -eq 0 ] ; then 
   echo "SQOOP job: $job existed and now is removed." >> $sqoop_job_creation_log
   sqoop job \
        --delete $jobname
fi
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
	--compress \
	--compression-codec org.apache.hadoop.io.compress.BZip2Codec \
	--query "${import_sql}" \
	--where "LAST_MOD_TS < '$sqoop_until'" \
	--target-dir $output_dir \
	--split-by $split_by \
	--hive-drop-import-delims \
	--null-string '' --null-non-string '' \
	--fields-terminated-by '\0x001' \
	--lines-terminated-by '\n' 
	
if [ $? -ne 0 ] ; then 
   echo "Failed to create SQOOP job: $job." >> $sqoop_job_creation_log
   exit 2
fi

echo "INFO Full Sqoop Job $jobname created
-------------------------------------------------------------------------------------------" >> $sqoop_job_creation_log

# Remove temp files from current directory
rm -f ./$table.*

cat $sqoop_job_creation_log
