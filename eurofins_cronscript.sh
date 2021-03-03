#!/usr/bin/env bash

module load anaconda2
source activate /apps/bio/repos/sars-cov-2-typing/eurofins-dl/conda-env/eurofins-dl

# Get eurofins files
/apps/bio/repos/sars-cov-2-typing/eurofins-dl/scripts/sync-sftp.sh "ftp-31100128759" "G5n6M3nk7op3!!!862"

conda deactivate
source activate vilma_general

# Fill empty cells in pangolin results file with NULL, checks files automatically, 24 hrs
/home/xcanfv/GBG_sars-cov-2-typing/sars-cov-2-typing/pangolin_fillemptyfield.py -a 

# Send files to mikro
/apps/bio/repos/sars-cov-2-typing/eurofins-dl/scripts/microReport.py

# Upload files to gms bucket, checks files automatically 24 hrs
/home/xcanfv/GBG_sars-cov-2-typing/sars-cov-2-typing/hcp_covid.py -b goteborg -ep https://vgtn0008.hcp1.vgregion.se:443 -aki Z290ZWJvcmc= -sak 50b94f5526cc530057af9694cf09a870 -a 
