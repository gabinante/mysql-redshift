#!/bin/bash

mydir="/opt/mysql-redshift"
date=`date '+%Y%m%d'`
pwfile="$mydir/.my.cnf"
blacklist_file="$mydir/blacklist.txt"
current_time=`date +%s`
table_schema='database'

#Create the table list
mysql --defaults-file="$mydir/.my.cnf" -N -s -r -e "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA='$table_schema';" > $mydir/all-tables.txt

#Grab the correct table order for import from yesterday's log
yesterdaylogpath=`ls -1tr $mydir/view-import-2017????.log | tail -1`
grep took $yesterdaylogpath | cut -d ' ' -f 3,5 | sort > $mydir/yesterdays-timing.txt

#Create the ordered whitelist
whitelist=`join -a 1 -e 0 -o 1.1,2.2 $mydir/all-tables.txt $mydir/yesterdays-timing.txt | sort -t ' ' -k 2,2nr | cut -d ' ' -f 1 | grep -v -f blacklist.txt`

# stop slave
mysql --defaults-file="$mydir/.my.cnf" -e "stop slave;"

#Iterate through table list and import to redshift. 4 threads
echo $whitelist | xargs -n 1 -P 4 sh $mydir/table_to_redshift.sh -d views -t

# start slave
mysql --defaults-file="$mydir/.my.cnf" -e "start slave;"

#email out daily import digest report
cat /tmp/import_digest_$(date +\%Y\%m\%d).txt | $mydir/digest.sh | mail -s " MySQL > RedShift Import complete" your@email.com
