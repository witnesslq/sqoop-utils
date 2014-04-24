#!/bin/bash
if [ $# -ne 2 ] ; then
   echo "USAGE: $0 [schema] [tablename]"
   exit
fi
BASEDIR=$(dirname $0)
source ${BASEDIR}/setenv.sh 
${BASEDIR}/sqoop_full_job_exec.sh $1 $2
