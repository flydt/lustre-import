#!/usr/bin/bash
#
# script to import files in lists to lustre directory
# lhsm_load.sh

if [ $# -lt 2 ];
then
    echo ${BASH_SOURCE} LUSTRE_IMPORT_PATH IMPORT_LIST_PATH IMPORT_BATCH_SIZE\(optional\)
    exit 1
fi

lustre_mnt_path=$1
import_list_path=$2

# import batch default size is 1000 items
batch_size=1000
if [ $# -eq 3 ];
then
	batch_size=$3
fi

echo import files to lustre to path "$lustre_mnt_path" with list file path "$import_list_path" and batch size "$batch_size"

loop_idx=1
task_done=0

while true; do

	echo loop "$loop_idx" start
	date

	list_num=`find $import_list_path -type f | wc -l`
	if [ $list_num -eq 0 ];
	then
		echo no list file to process in directory "$import_list_path"
		task_done=1
	else
		find $import_list_path -type f -print0 | xargs -0 -n 1 -P 8 -I {} build/lhsm_import $lustre_mnt_path {} $batch_size
	fi

	echo loop "$loop_idx" finished

	# check if import task finised, ie. all of list file processed
	list_num=`find $import_list_path -type f | wc -l`
	if [ $list_num -eq 0 ];
	then
		task_done=1
	fi

	if [ $task_done -eq 1 ];
	then
		echo import files to lustre to path "$lustre_mnt_path" with list file path "$import_list_path" finished
		break
	else
		date
		echo error happend in import, try again
		sleep 5
	fi

done

date
exit 0
~

