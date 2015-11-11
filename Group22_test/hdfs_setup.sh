#!/bin/bash

answer=1
while [ ${answer} -lt 4 ] && [ ${answer} -gt 0 ]
do
	echo -e "Please select option  \n1. Copy Input Files to hdfs \n2. Remove Files from hdfs \n3. get files from hdfs output \nanything else to exit "
	read answer

	if [ ${answer} -eq 1 ]; then
		#make directory for hdfs for input data
		hadoop fs -mkdir  hdfs://$ip:54310/$input_dir/
		ret=`echo $?`
		if [ ${ret} -eq 1 ]; then
			fileno=`hadoop fs -ls  hdfs://$ip:54310/$input_dir/ | wc -l`
			echo "input files and directory Already exsists!!! No of input file: ${fileno}"
			exit 0;
		else 
			hadoop fs -put  $input_dir/* hdfs://$ip:54310/$input_dir/
			fileno=`hadoop fs -ls  hdfs://$ip:54310/$input_dir/ | wc -l`
			echo "${fileno} number of files copied"
		fi
	elif [ ${answer} -eq 2 ]; then
		hadoop fs -rm  hdfs://$ip:54310/$input_dir/*
		hadoop fs -rmdir hdfs://$ip:54310/$input_dir
		hadoop fs -rm  hdfs://$ip:54310/${output_dir}/*/*
		hadoop fs -rmdir  hdfs://$ip:54310/${output_dir}/*
		hadoop fs -rmdir  hdfs://$ip:54310/${output_dir}/
		echo "Remove files and directory in hadoop"
	elif [ ${answer} -eq 3 ]; then
		hadoop fs -get  hdfs://$ip:54310/$output_dir/* ${output_dir}/
		echo "Output files retrieved from lhdfs to local"
	else
		echo "Exiting HDFS Operations"
	fi
done
