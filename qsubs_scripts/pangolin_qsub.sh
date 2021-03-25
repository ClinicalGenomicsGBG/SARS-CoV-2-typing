#!/bin/bash -l

#$ -cwd
#$ -o /medstore/logs/pipeline_logfiles/sars-cov-2-typing/pangolin_qsub.log
#$ -j y
#$ -S /bin/bash
#$ -pe mpi 1
#$ -q batch.q
#$ -l excl=1

OUTDIR='/medstore/results/clinical/SARS-CoV-2-typing/pangolin_results'
FASTAIN=$1
RUNID=$2

module load anaconda2
source activate /apps/bio/software/anaconda2/envs/pangolin

#Start pangolin on all samples
BASE=$(basename $FASTAIN)
pangolin \
    -t 1 \
    $FASTAIN \
    --outfile ${OUTDIR}/${RUNID}/${BASE%.primer*}_lineage_report.txt
