#!/usr/bin/env bash

FTPUSER=$1
FTPKEY=$2

module load anaconda2
source activate /apps/bio/repos/sars-cov-2-typing/eurofins-dl/conda-env/eurofins-dl

# Get eurofins files
/apps/bio/repos/sars-cov-2-typing/eurofins-dl/scripts/sync-sftp.sh $FTPUSER $FTPKEY 

conda deactivate
source activate vilma_general

# Fill empty cells in pangolin results file with NULL, checks files automatically, 24 hrs
/apps/bio/repos/sars-cov-2-typing/pangolin_fillemptyfield.py -a 

# Send files to mikro
/apps/bio/repos/sars-cov-2-typing/eurofins-dl/scripts/microReport.py

# Upload files to gms bucket, checks files automatically 24 hrs
/apps/bio/repos/sars-cov-2-typing/hcp_covid.py -b goteborg -ep https://vgtn0008.hcp1.vgregion.se:443 -aki Z290ZWJvcmc= -sak 50b94f5526cc530057af9694cf09a870 -e 
