#!/usr/bin/env python3

'''
A sub script to be used in Iontorrent dumping scripts

Given a list of samples (sample names), navigate thorough appropriate DIRs
and retreive the fastq seqs

'''

import sys
import glob
import os
import re
import shutil
import gzip

#---------------------------------------------------------------------------------#

root_path = "/seqstore/instruments/s5_01_mik/covid_datadump/"

''' get the list of samples '''
consensus_file_paths = glob.glob(os.path.join(root_path, '*.consensus.fasta'))  # Select an arbitrary filetype, just any of them
basenames = [os.path.basename(path) for path in consensus_file_paths]
sample_names = set([basename.split('.')[0].split('_')[-1] for basename in basenames])  # Ugly split but should work. Set removes duplicates


 
'''select a small list of samples for trial, this should be removed later '''
'''
sub_sample = []
count = 0
for elem in iter(sample_names):
    count = count + 1
    sub_sample.append(elem)
    if count == 6:
        break
'''

print("******************************************************")
print('-----Save absolute paths of matching fastq files -----')

fastq_path = '/seqstore/instruments/s5_01_mik/' 
file_path = []

for i in sample_names:
    fastq_abs_path = glob.glob(os.path.join(fastq_path, '**', f'*{i}*.fastq'), recursive=True)
    file_path.append(fastq_abs_path)
    print(f'{fastq_abs_path}')



print("**********************************************")
print('----Compress files and save in temp folder----')

tmp_root = "/home/xkocsu/for_test/"  ## IMP!  CHANGE THIS TO A TEMP FLDR IN /COVID_DATADUMP
for x in file_path:
    for y in x:
        with open(y, 'rb') as f_in:
            x_name = [y.split('.')[0].split('_')[5]] [0]  ## get the samplename
            with gzip.open(tmp_root + f'{x_name}.fastq.gz', 'wb') as f_out:
               shutil.copyfileobj(f_in, f_out)




#--------------------------------------------------------------------------------#




