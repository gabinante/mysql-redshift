#!/bin/bash
#
while read line
do
table=`echo $line | awk '{print $3}'`
rows=`echo $line | awk '{print $6}'`
redshift_rows=`echo $line | awk '{print $11}'`


if [[ $redshift_rows -gt $rows ]];
	then
#		echo " ERROR:  $redshift_rows rows loaded is greater than $rows rows from view $table"
		failed_load_array+=($table)
	elif
		 [[ $redshift_rows != $rows ]];
		then
	#	echo " ERROR: row totals do not match for table $table" 
		failed_load_array+=($table)
	else
		okay_load_array+=($table)
		
	#echo "$table load complete; $redshift_rows/$rows rows" 
	
	fi
done

if [[ ${#failed_load_array[@]} = 0 ]]
then
echo "all tables loaded successfully"
exit

else
echo "FAILED TABLES: The following ${#failed_load_array[@]} tables did not load, or failed to load completely."
	printf '%s\n' "${failed_load_array[@]}"
echo  " "
echo  " "
echo  " "

echo "SUCCESSFUL TABLES: the following ${#okay_load_array[@]} tables loaded successfully"
	#echo ${okay_load_array[@]}
	printf '%s\n' "${okay_load_array[@]}"
fi
