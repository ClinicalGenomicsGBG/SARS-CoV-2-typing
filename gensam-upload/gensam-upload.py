#!/usr/bin/env python

import click
import datetime
import os
import sys
import glob
from sample_sheet import SampleSheet
from collections import defaultdict

@click.command()
@click.option('-r', '--runid', required=True,
              help='RUNID of the seqrun to upload')
@click.option('-d', '--demultiplexdir', required=True,
              default='/seqstore/instruments/nextseq_500175_gc/Demultiplexdir',
              help='Path to demultiplexdir')
@click.option('-i', '--inputdir', required=True,
              default='/medstore/results/clinical/SARS-CoV-2-typing/nextseq_data',
              help='Path to location of files to upload')
@click.option('-s', '--samplesheetname', required=True,
              default='SampleSheet.csv',
              help='Name of SampleSheet file')
@click.option('--regioncode', required=True,
              default='14',
              help='FOHM region code')
@click.option('--labcode', required=True,
              default='SE300',
              help='FOHM lab code')
@click.option('-l', '--logdir', required=True,
              default='/medstore/logs/pipeline_logfiles/sars-cov-2-typing/GENSAM-upload',
              help='Path to directory where logs should be created')
def main(runid, demultiplexdir, logdir, inputdir, samplesheetname, regioncode, labcode):
    # Start the logging
    now = datetime.datetime.now()
    logfile = os.path.join(logdir, "GENSAM-upload_" + now.strftime("%y%m%d_%H%M%S") + ".log")
    logfile = os.path.join(logdir, "GENSAM-upload_debug.log")

    log = open(logfile, "a")
    log.write("----\n")
    log.write(writelog("LOG", "Starting GENSAM upload workflow"))

    #Get the path to samplesheet
    sspath = os.path.join(demultiplexdir, runid, samplesheetname)

    #Make sure the samplesheet exists
    if os.path.isfile(sspath):
        log.write(writelog("LOG", "Reading in all sample names from " + samplesheetname)) 
    else:
        log.write(writelog("ERROR", "Could not find SampleSheet @ " + sspath))
        sys.exit("ERROR: Could not find SamleSheet @ " +  sspath)
     
    #Read in all sampleIDs
    samples = sample_sheet(sspath)
    
    #Get a list of all files to upload
    #Sanity checks
    syncdict = defaultdict(lambda: defaultdict(dict))
    for dirname in ("fastq", "fasta", "lineage"):
        log.write(writelog("LOG", "Finding all " + dirname + " files to upload."))
        dirpath = os.path.join(inputdir, runid, dirname)
        if os.path.exists(dirpath):       
            for sample in samples:
                dirpath = os.path.join(inputdir, runid, dirname)
                if dirname == 'fasta':
                    for fastafile in glob.glob(dirpath + "/" + sample + "*consensus.fa"):
                        #Create symlink names
                        targetlink = os.readlink(fastafile)
                        samplename = sample.replace("_", "-") + '.consensus.fasta'
                        fastauploadname = '_'.join((regioncode, labcode, samplename))
                        #Store info in dict
                        syncdict[sample]['fasta'] = fastauploadname
                
                #elif dirname == 'fastq':
                    
                #elif dirname == 'lineage':
            
        else:
            log.write(writelog("ERROR", "No " + dirname + " files found."))
            

    #Upload all symlinks to the FOHM FTP

    #Finished the workflow
    log.write(writelog("LOG", "Finished GENSAM upload workflow."))
    log.close()

def sample_sheet(sspath):
    Sheet = SampleSheet(sspath)
    data = []
    for sample in Sheet.samples:
        sample_name = sample['Sample_Name']
        #Skip controls
        if sample_name.startswith(('NegCtrl', 'PosCtrl', 'PosKon', 'NegKon')):
            continue
        else:
            data.append(sample_name)

    return data
    
def writelog(logtype, message):
    now = datetime.datetime.now()
    logstring = "[" + now.strftime("%Y-%m-%d %H:%M:%S") + "] - " + logtype + " - " + message + "\n"
    return logstring

    
if __name__ == '__main__':
    main()
