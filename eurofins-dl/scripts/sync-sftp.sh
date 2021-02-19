#!/usr/bin/env bash

#Check that arguments have been passed
if [ "$#" -lt 2 ]; then
    echo "Usage: sync-sftp.sh <ftp username> <ftp password>"
fi

LOGFILE='/medstore/logs/pipeline_logfiles/sars-cov-2-typing/eurofins-dl.log'
DATALOC='/medstore/results/clinical/SARS-CoV-2-typing/eurofins_data/'
CURRENTTIME=$(date "+%Y-%m-%d %H:%M:%S")
FTPUSER=$1
FTPKEY=$2

# Print date to log
echo "** LOG: Starting sFTP sync @ $CURRENTTIME" >> $LOGFILE

# Sync the sFTP
# Uses lftp. Now produces quite a lot of log, one line per file
# Can be lowered be setting -vv or -v
lftp -e 'mirror -vvv / /medstore/results/clinical/SARS-CoV-2-typing/eurofins_data/; bye' \
  -u $FTPUSER,$FTPKEY \
  ftp.gatc-biotech.com:21 \
  &>> $LOGFILE

# Check the MD5sums of the recently downloaded files
CURDIR=$(pwd)
echo "** LOG: Completed mirror of sFTP. Starting MD5SUM check." >> $LOGFILE
#find files which was modified since download started. This finds only new md5sums.txt
find $DATALOC -name "md5sums.txt" -newerct "$CURRENTTIME" | while read x; do \
  cd ${x%md5sums.txt}
  md5sum -c $x >> $LOGFILE
  EXITSTATUS=$?
  if [ $EXITSTATUS -ne 0 ]; then
    echo "** ERROR: found in md5sums" >> $LOGFILE
  else
    echo "** LOG: All md5sums correct" >> $LOGFILE
  fi
done
cd $CURDIR

# Rename pangolin file to have a unique identifier (same as plate name)
echo "** LOG: Making renamed copy of pangolin file." >> $LOGFILE
find $DATALOC -name "CO*_pangolin_lineage_classification.txt" -newerct "$CURRENTTIME" | while read x; do \
  echo "** LOG: found un-renamed pangolin file $x" >> $LOGFILE
  FILENAME=$(basename $x)
  BASE=${x%/$FILENAME}
  PLATEID=${BASE##*/}
  # This makes a copy Change to mv if we don't need original
  new_x=${BASE}/${PLATEID}_$FILENAME
  echo "** LOG: Renaming to $new_x" >> $LOGFILE
  cp $x $new_x &>> $LOGFILE
  EXITSTATUS=$?
  if [ $EXITSTATUS -ne 0 ]; then
    echo "** ERROR: Problem when copying file $x" >> $LOGFILE
  fi
done

#End log
CURRENTTIME=$(date "+%Y-%m-%d %H:%M:%S")
echo "** LOG: Finished sFTP sync @ $CURRENTTIME" >> $LOGFILE
echo "-----" >> $LOGFILE
