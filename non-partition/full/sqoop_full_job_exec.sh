#!/bin/bash
if [ $# -ne 2 ] ; then
   echo "USAGE: $0 [schema] [tablename]"
   exit
fi

schema=`echo $1 | tr '[a-z]' '[A-Z]'`
table=`echo $2 | tr '[a-z]' '[A-Z]'`
jobtype='FULL'
jdbc_url=$SQOOP_JDBC_URL
output_path=$SQOOP_OUTPUT_PATH
backup_path=$SQOOP_BACKUP_PATH
log_path=$SQOOP_LOG_PATH

now_date=`date +"%Y-%m-%d"`
now_time=`date +"%H.%M.%S"`
user=`whoami | tr '[a-z]' '[A-Z]'`

tablename="$schema.$table"
jobname="$jobtype.$schema.$table"
output_dir=$output_path/$schema.$table
sqoop_job_execution_log=$log_path/SQOOP_FULL_JOB_EXECUTION_${schema}-${table}_${now_date}_${now_time}.log


echo "-------------------------------------------------------------------------------------------
 		        	FULL SQOOP JOB EXECUTION 
-------------------------------------------------------------------------------------------
INFO Script $0
INFO Job $jobname
INFO Job-type $jobtype 
INFO Target $output_dir" > $sqoop_job_execution_log

################################################################################
# Check if user has permissions 
################################################################################
if [ $user != $SQOOP_USER ] ; then
   echo "WARN $user tried to run this script" >> $sqoop_job_execution_log
   echo "You do not have permission to run this script. Please use userid hdobudw"
   exit 1
fi

################################################################################
# Check if output backup directory already exists
################################################################################
echo "Check if output backup directory $output_path/$schema.$table.backup already exists ..." >> $sqoop_job_execution_log
if hadoop fs -test -d $output_path/$schema.$table.backup; then
   hadoop fs -rm -r -skipTrash $output_path/$schema.$table.backup
   echo "Removed $output_path/$schema.$table.backup to Trash." >> $sqoop_job_execution_log
fi

################################################################################
# Check if output directory already exists
################################################################################
echo "Check if output directory $output_path/$schema.$table already exists ..." >> $sqoop_job_execution_log
if hadoop fs -test -d $output_dir; then
   echo "WARN Output directory already exists, moving it to backup directory" >> $sqoop_job_execution_log
   hadoop fs -mv $output_path/$schema.$table $output_path/$schema.$table.backup
   echo "Backup: moved $output_path/$schema.$table to $output_path/$schema.$table.backup" >> $sqoop_job_execution_log
fi

if [ -e $table.java ] ; then
   rm -f $table.java
   echo "WARN: removed $table.java"
fi

################################################################################
# Full Sqoop Job execution 
################################################################################
# Check if job exists
# TODO Add exit here if job does not exist
sqoop job \
	--show $jobname \
	2>  /dev/null > /dev/null
if [ $? -ne 0 ] ; then 
   echo "$jobname is not defined in sqoop. Exection failed." >> $sqoop_job_execution_log
   exit 2
fi

# Execute job
sqoop job \
      --exec $jobname \
      >> $sqoop_job_execution_log

if [ $? -ne 0 ] ; then 
   echo "Sqoop job exection failed: $jobname" >> $sqoop_job_execution_log
   hadoop fs -mv $output_path/$schema.$table.backup $output_path/$schema.$table
   echo "Rollback: moved $output_path/$schema.$table.backup to $output_path/$schema.$table" >> $sqoop_job_execution_log
   exit 3
else 
   echo "Sqoop job exection succeeded: $jobname" >> $sqoop_job_execution_log
   hadoop fs -rm -r $backup_path/$schema.$table.*
   echo "Removed $backup_path/$schema.$table.* to Trash." >> $sqoop_job_execution_log
   hadoop fs -mv $output_path/$schema.$table.backup $backup_path/$schema.$table.backup
   echo "Backup: moved $output_path/$schema.$table.backup to $backup_path/$schema.$table.backup" >> $sqoop_job_execution_log
fi


echo "INFO Full Sqoop Job $jobname executed 
-------------------------------------------------------------------------------------------" >> $sqoop_job_execution_log

# Remove temp files from current directory
rm -f ./$table.*

cat $sqoop_job_execution_log
