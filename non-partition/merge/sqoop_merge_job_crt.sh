#!/bin/bash
if [ $# -ne 4 ] ; then
   echo "USAGE: $0 [schema] [tablename] [merge-key] [number-reducers]"
   exit
fi

schema=`echo $1 | tr '[a-z]' '[A-Z]'`
table=`echo $2 | tr '[a-z]' '[A-Z]'`
merge_key=`echo $3 | tr '[a-z]' '[A-Z]'`
nbr_reducers=$4

# These variables should be changed for specific use 
# Make sure to not have a trailing / after the path
# ----------------------------------------------------------------------------
jar_path=$SQOOP_JAR_PATH
log_path=$SQOOP_LOG_PATH
full_output_path=$SQOOP_FULL_OUTPUT_PATH
incr_output_path=$SQOOP_INCR_OUTPUT_PATH
merge_output_path=$SQOOP_MERGE_OUTPUT_PATH

now_date=`date +"%Y-%m-%d"`
now_time=`date +"%H.%M.%S"`
user=`whoami | tr '[a-z]' '[A-Z]'`

tablename="$schema.$table"
jar_file=$jar_path/$table.jar
old_data=$full_output_path/$schema.$table
new_data=$incr_output_path/$schema.$table
output_dir=$merge_output_path/$schema.$table
data_dir=$full_output_path/$schema.$table
sqoop_merge_job_creation_log=$log_path/SQOOP_MERGE_${schema}-${table}_${now_date}_${now_time}.log
jobname="MERGE.$schema.$table"


echo "-------------------------------------------------------------------------------------------
 		        	MERGE SQOOP JOB CREATION
-------------------------------------------------------------------------------------------
INFO Script $0
INFO Table $tablename
INFO Number_Of_Reducers $nbr_reducers
INFO JAR   $jar_file
INFO OLD_DATA_DIR $old_data
INFO NEW_DATA_DIR $new_data
INFO MERGE_DATA_DIR $output_dir
INFO Target $output_dir" > $sqoop_merge_job_creation_log

################################################################################
# Check if user has permissions 
################################################################################
if [ $user != $SQOOP_USER ] ; then
   echo "WARN $user tried to run this script" >> $sqoop_merge_job_creation_log
   echo "You do not have permission to run this script. Please use userid hdobudw"
   exit 1
fi


################################################################################
# Sqoop incremental job creation 
################################################################################
# Use --verbose to see a detailed log of sqoop import command

# Delete job if already exists
echo "Delete $jobname if already exists."
sqoop job \
    --show $jobname \
    2>  /dev/null > /dev/null
exists=$?
if [ $exists -eq 0 ] ; then 
   sqoop job \
	--delete $jobname 
echo "Existing job $jobname has been deleted."
fi

# Create job
sqoop job -Dpool.name=sqoop \
	-Dmapred.job.name=$jobname \
	-Dmapred.reduce.tasks=$nbr_reducers \
	-Dmapred.output.compress=true \
	-Dmapred.output.compression.codec="org.apache.hadoop.io.compress.BZip2Codec" \
	--create $jobname \
	-- merge \
	--new-data $new_data \
	--onto $old_data \
	--target-dir $output_dir \
	--merge-key $merge_key \
	--jar-file $jar_file \
	--class-name $table


echo "INFO Merge Sqoop Job $jobname created 
-------------------------------------------------------------------------------------------" >> $sqoop_merge_job_creation_log

cat $sqoop_merge_job_creation_log
