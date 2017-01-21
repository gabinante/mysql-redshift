This job imports data from a mysql database into a redshift data warehouse. It makes a couple of assumptions about your setup:
1. You're using views to scrub data before it enters your redshift environment (although it has an alternate method if you want to use mysqldump)

Dependencies:
s3 bucket redshift-import
s3 access for specified aws access and secret keys
redshift database and schema specified in table-to-redshift.sh
credential dotfiles in this directory specified in table-to-redshift.sh
blacklist.txt of any tables NOT allowed for import to the data warehouse in this directory
mysql database running on this host (although it would be quite easy to adapt this to run on an external box as well.)
psql client

import-wrapper.sh is a script that invokes the other
scripts in the directory.

the main player in the redshift import is table-to-redshift.sh, which is fed tables from a sorted list on import-wrapper.sh. Commenting out the xargs command in import_wrapper lets you run a single table if you choose.
