#!/bin/bash

cd /home/nwis_user/copy_sitefile

export date_suffix=`date +%Y%m%d_%H%M`

export DIR=/srv/mysql-data/spool
export ORACLE_HOME=/usr/oracle/app/oracle/product/11.2.0/client_1
export PATH=$PATH:$ORACLE_HOME/bin
export success_notify="bheck@usgs.gov,barry_heck@yahoo.com"
export failure_notify="bheck@usgs.gov,barry_heck@yahoo.com"
export TNS_ADMIN=/usr/local/etc

(
tries=0
while [ $tries -lt 6 ] ; do

   time mysql --quick nwisweb --user=nwis_user --password=***REMOVED*** < capture_counts.sql | grep -v "count(" > capture1.txt
   time mysql --quick nwisweb --user=nwis_user --password=***REMOVED*** < QW_RESULT.sql      | sed 's/\\\\/\//' > $DIR/QW_RESULT.out
   time mysql --quick nwisweb --user=nwis_user --password=***REMOVED*** < QW_SAMPLE.sql      | sed 's/\\\\/\//' > $DIR/QW_SAMPLE.out
   time mysql --quick nwisweb --user=nwis_user --password=***REMOVED*** < SITEFILE.sql       | sed 's/\\\\/\//' | sed  's/'`echo -e "\\0342\\0200\\0231"`'/'`echo -e "\\047"`'/g' > $DIR/SITEFILE.out
   time mysql --quick nwisweb --user=nwis_user --password=***REMOVED*** < SERIES_CATALOG.sql | sed 's/\\\\/\//' > $DIR/SERIES_CATALOG.out
   time mysql --quick nwisweb --user=nwis_user --password=***REMOVED*** < capture_counts.sql | grep -v "count(" > capture2.txt
   
   diffs=`diff capture1.txt capture2.txt | wc -l`

   res1=`wc -l $DIR/QW_RESULT.out | awk '{print $1}'`
   res1a=`expr $res1 - 1`
   res1b=`head -1 capture1.txt`

   res2=`wc -l $DIR/QW_SAMPLE.out | awk '{print $1}'`
   res2a=`expr $res2 - 1`
   res2b=`head -2 capture1.txt | tail -1`

   res3=`wc -l $DIR/SITEFILE.out | awk '{print $1}'`
   res3a=`expr $res3 - 1`
   res3b=`head -3 capture1.txt | tail -1`

   res4=`wc -l $DIR/SERIES_CATALOG.out | awk '{print $1}'`
   res4a=`expr $res4 - 1`
   res4b=`tail -1 capture1.txt`

   if [ $diffs -eq 0 -a $res1a -eq $res1b -a $res2a -eq $res2b -a $res3a -eq $res3b -a $res4a -eq $res4b ] ; then
      break;
   else
      echo diffs found `date`
      diff capture1.txt capture2.txt || [ $? -le 2 ]
      echo $res1a $res1b $res2a $res2b $res3a $res3b $res4a $res4b
   fi

   if [ $tries -ge 5 ] ; then
      echo "data changing or other mismatches - exit"
      ( echo "nad load has failed.  Data changing in tables or other mismatch."
        echo 
        echo before:
        echo 
        cat capture1.txt
        echo 
        echo after:
        echo 
        cat capture2.txt
        echo "verifications: "$res1a" "$res1b" "$res2a" "$res2b" "$res3a" "$res3b" "$res4a" "$res4b ) | 
      mail -s "nad load failed in extract" $failure_notify
      exit 1
   fi
   tries=`expr $tries + 1`
   sleep 300

done

cd /home/nwis_user/copy_sitefile

date
) 2>&1 | tee -a spool_out_all_$date_suffix.out
