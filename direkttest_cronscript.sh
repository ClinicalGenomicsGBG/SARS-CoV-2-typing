#!/usr/bin/env bash

module load anaconda2
source activate vilma_general

# Fix excel files with direktdata
/apps/bio/repos/sars-cov-2-typing/direkttest_csv.py -a

# Upload files to gms bucket, checks files automatically 24 hrs
/apps/bio/repos/sars-cov-2-typing/hcp_covid.py -b goteborg -ep https://vgtn0008.hcp1.vgregion.se:443 -aki Z290ZWJvcmc= -sak 50b94f5526cc530057af9694cf09a870 -d 
