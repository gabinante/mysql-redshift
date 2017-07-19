#!/bin/bash

# This adds colors for xargs so you can tell your threads apart.
#RED
arr[0]="tput setaf 1"
#GREEN
arr[1]="tput setaf 2"
#YELLOW
arr[2]="tput setaf 3"
#LIME_YELLOW
arr[3]="tput setaf 190"
#BLUE
arr[4]="tput setaf 4"
#POWDER_BLUE
arr[5]="tput setaf 153"
#MAGENTA
arr[6]="tput setaf 5"
#CYAN
arr[7]="tput setaf 6"
#WHITE
arr[8]="tput setaf 7"

for (( i = 0; i < ${#arr[@]} ; i++ )); do
  if mkdir /tmp/arr$i-lock 2> /dev/null; then
    eval ${arr[$i]}
    echo "color lock gained for xargs thread $i"
    break
  fi
done


mydir="/opt/mysql-redshift"
total=0

if [ ! $1 ] ; then
  echo "Usage: $0 -d/-db <database name> -t/-table <table name>"
fi

while getopts d:t:db:table: opt; do
  case $opt in
  d | db)
      mydb=$OPTARG
      ;;
  t | table)
      mytable=$OPTARG
      ;;
  esac
done

shift $((OPTIND - 1))

if [ ! $mydb ] ; then
  echo "Schema name required!"
  exit
fi

if [ ! $mytable ] ; then
  echo "Table name required!  Here are all available tables in schema $mydb:"
  mysql --defaults-file="$mydir/.my.cnf" -e "show tables from $mydb"
  exit
fi

date=`date '+%Y%m%d'`


#Load AWS credentials
access_key=`cat $mydir/.accesskey`
secret_key=`cat $mydir/.secretkey`

mydatadir="/opt/mysql-redshift/redshift-$date"
s3bucket='redshift-import-bucket'
s3folder="datawarehouse-$date"
redshift_host='example-redshift.c74dgrxrnj3n.us-west-1.redshift.amazonaws.com'
redshift_port='5439'
redshift_dbname='redshiftdb'
redshift_schema='redshiftschema'
redshift_user='awsuser'
redshift_password=`cat $mydir/.redshiftpass`
redshift_options='requiressl=true'
redshift_param="delimiter '\t' maxerror 500 ignoreblanklines gzip escape"
maxrows=1000

#Placeholder in case we ever add a where clause
whereclause="1=1"

if [ ! -d $mydatadir ]; then
	echo "Creating directory $mydatadir"
	mkdir $mydatadir
	chmod go+w $mydatadir
	chgrp mysql $mydatadir
fi

echo $(date '+%F %T') Processing $mytable
starttime=`date +%s`
rm -f $mydatadir/$mytable.*

rows=`mysql --defaults-file="$mydir/.my.cnf" -N -e "select count(*) from $mydb.$mytable WHERE $whereclause"`
dumpstart=`date +%s`
echo "$(date '+%F %T') Doing MySQL dump of $rows rows from $mytable..."

#Dump of a view creates a "CREATE VIEW" statement, so you need to select * to an outfile if you're using a views schema
echo `mysql --defaults-file="$mydir/.my.cnf" -N -e "select * from $mydb.$mytable WHERE $whereclause INTO OUTFILE '$mydatadir/$mytable.txt'"`

#If you're not using views to scrub your data, you can mysqldump the table.
#mysqldump -u$myuser -p$mypassword -h$myhost $myoptions $mydb $mytable -tab=$mydatadir > $mydatadir/mydump-$mytable.txt

echo "$(date '+%F %T') MySQL dump of $mytable complete."

zipstart=`date +%s`
#Zip up the table before loading to s3
echo "$(date '+%F %T') Zipping dump of $mytable..."
gzip $mydatadir/$mytable.txt

#Push to s3 bucket
echo "$(date '+%F %T') Loading $mytable.txt.gz to S3..."
s3start=`date +%s`
command_out=$( aws s3 cp $mydatadir/$mytable.txt.gz s3://$s3bucket/$s3folder/$mytable.txt.gz 2>&1 )
command_rc=$?

#Retry if load to s3 failed
if [ "$command_rc" = "0" ]
then
        echo "$(date '+%F %T') $mytable.txt.gz loaded to S3"
else
        echo "$(date '+%F %T') Error loading $mytable.txt.gz to S3. rc: $command_rc, stdout and stderr: $command_out"
	echo "$(date '+%F %T') $mytable.txt.gz, retrying" | mail -s 'S3 load error' your@email.com
	echo "$(date '+%F %T') 2nd attempt loading $mytable.txt.gz to S3..."
	 aws s3 cp $mydatadir/$mytable.txt.gz s3://$s3bucket/$s3folder/$mytable.txt.gz
fi

#remove zip file for local host once it's loaded to s3
rm -f $mydatadir/$mytable.txt.gz

# Statement to truncate table
cat > $mytable.truncate-table.php << heredoc
<?php
\$conn=pg_connect("host=$redshift_host port=$redshift_port $redshift_options dbname=$redshift_dbname user=$redshift_user password=$redshift_password");
\$str="truncate table $redshift_schema.$mytable;";
pg_query(\$conn, \$str);
?>
heredoc

#Create redshift load PHP file. This is a bit awkward, we did it this way so that we don't need a postgres client installed.
echo "copy $redshift_schema.$mytable from 's3://$s3bucket/$s3folder/$mytable.txt' CREDENTIALS 'aws_access_key_id=$access_key;aws_secret_access_key=$secret_key' $redshift_param ; " > $mytable.redshift-load

loadstart=`date +%s`

cat > $mytable.redshift.php << heredoc
<?php
\$conn=pg_connect("host=$redshift_host port=$redshift_port $redshift_options dbname=$redshift_dbname user=$redshift_user password=$redshift_password");
\$str=file_get_contents("$mytable.redshift-load");
pg_query(\$conn, \$str);
?>
heredoc

#Truncate the table using the php file created above
php $mytable.truncate-table.php

echo "$(date '+%F %T') Running Redshift Copy to load table $mytable ..."
#Load table into redshift via php.

php $mytable.redshift.php

#Delete the temporary load statements we created
rm -f $mytable.redshift.php
rm -f $mytable.redshift-load
rm -f $mytable.truncate-table.php

#Ended up installing a postgres client anyways, makes connect strings more readable and friendly. This is just for logging, can be removed if you dont want to install psql
import_row_count=`psql -A -t -h $redshift_host -d $redshift_dbname -p $redshift_port -U $redshift_user --no-password -c "select count(*) from $redshift_schema.$mytable"`

echo "$(date '+%F %T') $mytable import completed; $import_row_count rows loaded out of $rows" | tee -a /tmp/import_digest_$(date +\%Y\%m\%d).txt
completetime=`date +%s`

fulltime=$(($completetime - $starttime))
importtime=$(($completetime - $loadstart))
s3time=$(($loadstart - $s3start))
ziptime=$(($s3start - $zipstart))
exporttime=$(($zipstart - $starttime))
echo "Complete load of $mytable took $fulltime seconds. Export to file took $exporttime seconds, zip took $ziptime seconds, load to s3 took $s3time seconds, and import to redshift from s3 took $importtime seconds."

# Free up our color for use by another thread
rm -r /tmp/arr$i-lock;
exit
