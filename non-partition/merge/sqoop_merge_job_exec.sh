#!/bin/bash
if [ $# -ne 2 ] ; then
   echo "USAGE: $0 [schema] [tablename]"
   exit
fi

schema=`echo $1 | tr '[a-z]' '[A-Z]'`
table=`echo $2 | tr '[a-z]' '[A-Z]'`


# These variables should be changed for specific use 
# Make sure to not have a trailing / after the path
# ----------------------------------------------------------------------------
log_path=$SQOOP_LOG_PATH
full_output_path=$SQOOP_FULL_OUTPUT_PATH
full_backup_path=$SQOOP_FULL_BACKUP_PATH
incr_output_path=$SQOOP_INCR_OUTPUT_PATH
merge_output_path=$SQOOP_MERGE_OUTPUT_PATH
merge_backup_path=$SQOOP_MERGE_BACKUP_PATH

user=`whoami | tr '[a-z]' '[A-Z]'`
now_date=`date +"%Y-%m-%d"`
now_time=`date +"%H.%M.%S"`
start_time=$(date +"%s")

tablename="$schema.$table"
jobtype='MERGE'
jobname="$jobtype.$schema.$table"
old_data=$full_output_path/$schema.$table
new_data=$incr_output_path/$schema.$table
output_dir=$merge_output_path/$schema.$table
data_dir=$full_output_path/$schema.$table
sqoop_job_execution_log=$log_path/SQOOP_MERGE_${schema}-${table}_${now_date}_${now_time}.log

echo "-------------------------------------------------------------------------------------------
 			             SQOOP MERGE SUMMARY
-------------------------------------------------------------------------------------------
INFO SCRIPT $0
INFO Table $tablename
INFO OldData $old_data
INFO NewData $new_data
INFO MergeDir $merge_output_path/$schema.$table
INFO Start Date ${now_date}_${now_time}" > $sqoop_job_execution_log

################################################################################
# Check if user has permissions 
################################################################################
if [ $user != $SQOOP_USER ] ; then
   echo "WARN $user tried to run this script" >> $sqoop_job_execution_log
   echo "You do not have permission to run this script. Please use userid $SQOOP_USER"
   exit
fi


################################################################################
# Check if incremental has any data
################################################################################
count=$(hadoop fs -cat ${new_data}/part-m* | wc -l)
if [ $count -eq 0 ] ; then
   echo "No need to merge since ${new_data} has no data." >> $sqoop_job_execution_log
   echo "No need to merge since ${new_data} has no data." 
   /usr/bin/mutt -c $SQOOP_EMAIL_FR_ADDR -s "$jobname succeeded: nothing to merge" -- $SQOOP_EMAIL_TO_ADDR <<EOT
EOT
   exit 0
fi 



################################################################################
# Check if merge output directory already exists
################################################################################
if hadoop fs -test -d $merge_output_path/$schema.$table; then
   echo "WARN Merge directory already exists, removing it." >> $sqoop_job_execution_log
   hadoop fs -rm -r -skipTrash $merge_output_path/$schema.$table 
   echo "Removed $merge_output_path/$schema.$table to Trash. " >> $sqoop_job_execution_log
fi

	
################################################################################
# Merge Sqoop Job execution 
################################################################################
echo "INFO SQOOP MERGE BEGIN $now_date $now_time" >> $sqoop_job_execution_log
# Check if job exists
# TODO Add exit here if job does not exist
sqoop job \
      --show $jobname \
      2>  /dev/null > /dev/null   

if [ $? -ne 0 ] ; then 
   echo "$jobname is not defined in sqoop. Exection failed." >> $sqoop_job_execution_log
   echo "$jobname is not defined in sqoop. Exection failed."
   /usr/bin/mutt -c $SQOOP_EMAIL_FR_ADDR -s "$jobname failed: job not defined" -- $SQOOP_EMAIL_TO_ADDR < $sqoop_job_execution_log
   exit 1
fi

sqoop job \
      --exec $jobname \
      >> $sqoop_job_execution_log 2>&1

if [ $? -ne 0 ] ; then 
   echo "$job exection failed." >> $sqoop_job_execution_log
   echo "$job exection failed." 
   /usr/bin/mutt -c $SQOOP_EMAIL_FR_ADDR -s "$jobname failed in execution" -- $SQOOP_EMAIL_TO_ADDR < $sqoop_job_execution_log
   exit 2
fi


################################################################################
# Copy merged data to full sqoop directory for next merge
################################################################################
# Remove the existing backup directory
if hadoop fs -test -d $full_backup_path/$schema.$table.backup; then
   echo "WARN Backup directory already exists, removing it." >> $sqoop_job_execution_log
   hadoop fs -rm -r -skipTrash $full_backup_path/$schema.$table.backup
   echo "Removed $full_backup_path/$schema.$table.backup to Trash." >> $sqoop_job_execution_log
fi
# Move existing output dierctory to  backup directory
if hadoop fs -test -d $full_output_path/$schema.$table; then
   hadoop fs -mv $full_output_path/$schema.$table $full_backup_path/$schema.$table.backup
   echo "Backup: moved $full_output_path/$schema.$table to $full_backup_path/$schema.$table.backup" >> $sqoop_job_execution_log
fi

# Move merged data to full output directory
hadoop fs -mv $merge_output_path/$schema.$table $full_output_path/$schema.$table 
echo "Moved $merge_output_path/$schema.$table to $full_output_path/$schema.$table" >> $sqoop_job_execution_log
echo "INFO Merge Sqoop Job $jobname executed
-------------------------------------------------------------------------------------------" >> $sqoop_job_execution_log

now_date=`date +"%Y-%m-%d"`
now_time=`date +"%H.%M.%S"`
end_time=$(date +"%s")
minutes=$(( ($end_time - $start_time) / 60 ))
echo "INFO End Date ${now_date}_${now_time}" >> $sqoop_job_execution_log
echo "INFO Duration in minutes: $minutes " >> $sqoop_job_execution_log

/usr/bin/mutt -c $SQOOP_EMAIL_FR_ADDR -s "$jobname succeeded in $minutes MIN" -- $SQOOP_EMAIL_TO_ADDR < $sqoop_job_execution_log
cat $sqoop_job_execution_log
