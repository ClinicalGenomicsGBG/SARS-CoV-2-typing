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


'''get the list of samples '''
# the below list should be updated/replaced by a "ls -l" output of /seqstore/instruments/s5_01_mik/covid_datadump folder
file_names = ["-rw-rw-r--. 1 xsikpe 1000 170K May 31 10:48 14_SE300_DC21-47608.vcf",
		"-rw-rw-r--. 1 xsikpe 1000  30K May 31 10:48 14_SE300_DC21-47608.consensus.fasta",
		"-rw-rw-r--. 1 xsikpe 1000 171K May 31 10:48 14_SE300_DC21-48476.vcf",
		"-rw-rw-r--. 1 xsikpe 1000  30K May 31 10:48 14_SE300_DC21-48476.consensus.fasta",
		"-rw-rw-r--. 1 xsikpe 1000 169K May 31 10:48 14_SE300_DC21-44761.vcf",
		"-rw-rw-r--. 1 xsikpe 1000  30K May 31 10:48 14_SE300_DC21-44761.consensus.fasta",
		"-rw-rw-r--. 1 xsikpe 1000 171K May 31 10:48 14_SE300_DC21-48667.vcf",
		"-rw-rw-r--. 1 xsikpe 1000  30K May 31 10:48 14_SE300_DC21-48667.consensus.fasta"]

sample_name = []
for i in file_names:
    if i.endswith("vcf"):  # select vcf files
        name1 = re.split('[_ .]', i)
        sample_name.append(name1[11])
        print(name1[11])


''' for every sample name, navigate through directories recursively and store path of *fastq files '''
dirpath = "/seqstore/instruments/s5_01_mik/"
out_dir = "/home/xkocsu/for_testing/"  ## can update this path and use it for dumping, prbably /seqstore/instruments/s5_01_mik/covid_datadump ??


''' retrieve the path of selected fastq files'''
print("**********************************************")
print('-----Select paths to specific fastq files-----')

#lst_file = ['DNA76012', 'DNA76029', 'DNA76136']  ## change this to the file names from excel sheet
listFilesD = [] # create an empty list for (path) fastq files

for dirName, subdirList, fileList in os.walk((dirpath), topdown=False):
    for files in fileList:
        for names in sample_name:  ## for every sample names:
            if names in files and files.endswith("fastq"):  ## match for patterns in file names
                if files not in listFilesD:
                    listFilesD.append(os.path.join(dirName, files)) # adds files from different directories
#                        print(files)

for i in listFilesD:
    print(i)



'''


'''
