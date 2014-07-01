#!/bin/bash
if [[ "$#" -ne "1" ]]; then
	echo "Invalid parameter count."
	echo "Usage: `basename $0` db_instance"
	exit 1;
fi

instance=$1

cd /home/nwis_user/copy_sitefile

export date_suffix=`date +%Y%m%d_%H%M`

export DIR=/srv/mysql-data/spool
export ORACLE_HOME=/usr/oracle/app/oracle/product/11.2.0/client_1
export PATH=$PATH:$ORACLE_HOME/bin
export success_notify="bheck@usgs.gov,barry_heck@yahoo.com"
export failure_notify="bheck@usgs.gov,barry_heck@yahoo.com"
export TNS_ADMIN=/usr/local/etc

(
export nwis_ws_star_pass=`cat .sp`

rm -f *.bad

time sqlldr userid=NWIS_WS_STAR@${instance} control=QW_RESULT.ctl data=$DIR/QW_RESULT.out direct=true skip=1 << EOT
$nwis_ws_star_pass
EOT
time sqlldr userid=NWIS_WS_STAR@${instance} control=QW_SAMPLE.ctl data=$DIR/QW_SAMPLE.out direct=true skip=1 << EOT
$nwis_ws_star_pass
EOT
time sqlldr userid=NWIS_WS_STAR@${instance} control=SITEFILE.ctl  data=$DIR/SITEFILE.out direct=true skip=1 << EOT
$nwis_ws_star_pass
EOT
time sqlldr userid=NWIS_WS_STAR@${instance} control=SERIES_CATALOG.ctl  data=$DIR/SERIES_CATALOG.out direct=true skip=1 << EOT
$nwis_ws_star_pass
EOT

if [ -f QW_RESULT.bad -o -f QW_SAMPLE.bad -o -f SITEFILE.bad -o -f SERIES_CATALOG.bad ] ; then
   echo ".bad files found - exit"
   ( echo "nad load has failed. .bad files found:"
     echo
     ls -l *.bad ) | 
   mail -s "nad load failed in extract" $failure_notify
   exit 1
fi

success=`grep "successfully loaded" QW_RESULT.log QW_SAMPLE.log SITEFILE.log SERIES_CATALOG.log | wc -l`
if [ -f QW_RESULT.bad -o -f QW_SAMPLE.bad -o -f SITEFILE.bad -o -f SERIES_CATALOG.bad ] ; then
   echo ".bad files found - exit"
   ( echo "nad load has failed. .bad files found:"
     echo
     ls -l *.bad ) | 
   mail -s "nad load failed in extract" $failure_notify
   exit 1
fi

cd /home/nwis_user/copy_sitefile

date
) 2>&1 | tee -a spool_out_all_$date_suffix.out
