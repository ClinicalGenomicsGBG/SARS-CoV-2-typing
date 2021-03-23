#!/usr/bin/env bash

module load anaconda2
source activate vilma_general

# Fill empty cells in pangolin results file with NULL and change taxon names, checks files automatically, 24 hrs
/apps/bio/repos/sars-cov-2-typing/pangolin_fillemptyfield.py -n 

# Change taxon names, specific for GENSAM, checks files automatically, 24 hrs
/apps/bio/repos/sars-cov-2-typing/pangolin_fillemptyfield.py -g 

# Send files to mikro
/apps/bio/repos/sars-cov-2-typing/nextseq/microReport.py

# Run samplesheet parser for metadata 
/apps/bio/repos/sars-cov-2-typing/samplesheet_parser.py --filepath <pathtofil>

# Upload consensus fasta files to CLC
/apps/bio/repos/sars-cov-2-typing/clc_sync.sh $RUN

# Upload files to gms bucket (fasta, fastq, pangolin and metadata), checks files automatically 24 hrs
/apps/bio/repos/sars-cov-2-typing/hcp_covid.py -b goteborg -ep https://vgtn0008.hcp1.vgregion.se:443 -aki Z290ZWJvcmc= -sak 50b94f5526cc530057af9694cf09a870 -n 

# Upload to GENSAM (fasta, fastq, pangolin)
# Send email when done
