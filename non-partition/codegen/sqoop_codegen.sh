#!/bin/bash
if [ $# -ne 2 ] ; then
   echo "USAGE: $0 [schema] [tablename]"
   exit
fi

schema=`echo $1 | tr '[a-z]' '[A-Z]'`
table=`echo $2 | tr '[a-z]' '[A-Z]'`
jdbc_url=$SQOOP_JDBC_URL
jdbc_passwd=$SQOOP_JDBC_PASSWORD
output_dir=$SQOOP_JAR_PATH
codegen_dir=$SQOOP_CODE_PATH
log_path=$SQOOP_LOG_PATH

# These variables should be changed for specific use
# Make sure not to have a trailing / after the path
# ----------------------------------------------------------------------------
conn=${jdbc_url}":currentSchema=$schema;"
# ----------------------------------------------------------------------------

now_date=`date +"%Y-%m-%d"`
now_time=`date +"%H.%M.%S"`
user=`whoami | tr '[a-z]' '[A-Z]'`

tablename="$schema.$table"
codegen_log=$log_path/SQOOP_CODEGEN_${schema}-${table}_${now_date}_${now_time}.log

echo "-------------------------------------------------------------------------------------------
                                     SQOOP CODEGEN SUMMARY
-------------------------------------------------------------------------------------------
INFO Script $0
INFO Table $tablename
INFO Source $conn
INFO Code   $codegen_dir
INFO Target $output_dir" > $codegen_log

################################################################################
# Check if user has permissions 
################################################################################
if [ $user != $SQOOP_USER ] ; then
   echo "WARN $user tried to run this script" >> $codegen_log
   echo "You do not have permission to run this script. Please use userid hdobudw"
   exit
fi

################################################################################
# Check if files already exist
################################################################################
if [ -e ${output_dir}/${table}.class ] || [ -e ${output_dir}/${table}.jar ] ; then 
   rm -f ${output_dir}/${table}.*
fi

# Remove temp files from current directory
rm -f $codegen_dir/$table.*

################################################################################
# Generate code-gen
################################################################################
echo "INFO Creating $table.jar file" >> $codegen_log
echo "-------------------------------------------------------------------------------------------" >> $codegen_log
# Use --verbose to see a detailed log of sqoop import command
sqoop codegen \
	--connect $conn \
	--username $user \
	--password $jdbc_passwd \
	--table $table \
	--bindir $output_dir \
	--outdir $codegen_dir \
	--fields-terminated-by '\0x001' \
	--lines-terminated-by '\n' \
	--null-string '' \
	--null-non-string '' \
	--hive-drop-import-delims \
	2>> $codegen_log

if [ $? -ne 0 ] ; then
   echo "Sqoop code_gen exection failed." >> $codegen_log
   cat $codegen_log
   exit 1
else
   echo "Sqoop code_gen exection succeeded." >> $codegen_log
   rm -f $output_dir/$table.class
   cat $codegen_log
   exit 0
fi
