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
    #Get the path to samplesheet
    sspath = os.path.join(demultiplexdir, runid, samplesheetname)

    #Run checks on all given inputs
    checkinput(runid, demultiplexdir, inputdir, regioncode, labcode, logdir, samplesheetname, sspath)
    
    # Start the logging
    now = datetime.datetime.now()
    logfile = os.path.join(logdir, "GENSAM-upload_" + now.strftime("%y%m%d_%H%M%S") + ".log")
    logfile = os.path.join(logdir, "GENSAM-upload_debug.log")

    log = open(logfile, "a")
    log.write("----\n")
    log.write(writelog("LOG", "Starting GENSAM upload workflow"))
 
    #Read in all sampleIDs
    samples = sample_sheet(sspath)

    #Get a list of all files to upload
    syncdict = defaultdict(lambda: defaultdict(dict))
    for dirname in ("fastq", "fasta", "lineage"):
        log.write(writelog("LOG", "Finding all " + dirname + " files to upload."))
        dirpath = os.path.join(inputdir, runid, dirname)
        for sample in samples:
            dirpath = os.path.join(inputdir, runid, dirname)
            if dirname == 'fasta':
                #Find all files to upload based on existing links, count them
                syncdict['fastacount'] = 0
                for fastafile in glob.glob(dirpath + "/" + sample + "*consensus.fa"):
                    #Create symlink names
                    targetlink = os.readlink(fastafile)
                    samplename = sample.replace("_", "-") + '.consensus.fasta'
                    fastauploadname = '_'.join((regioncode, labcode, samplename))
                    #Store info in dict
                    syncdict[sample]['fasta'] = fastauploadname
                    syncdict['fastacount']  += 1

                #elif dirname == 'fastq':
                    
                #elif dirname == 'lineage':
            

    #Upload all symlinks to the FOHM FTP

    #Finished the workflow
    log.write(writelog("LOG", "Finished GENSAM upload workflow."))
    log.close()

def checkinput(runid, demultiplexdir, inputdir, regioncode, labcode, logdir, samplesheetname, sspath):
    #Make sure the samplesheet exists
    if not os.path.isfile(sspath):
        sys.exit("ERROR: Could not find SamleSheet @ " +  sspath)

    #Check for all fasta, fastq and lineage directories are in place
    for dirname in ("fastq", "fasta", "lineage"):
        dirpath = os.path.join(inputdir, runid, dirname)
        if not os.path.exists(dirpath):
            sys.exit("ERROR: No " + dirname + " directory found.")

    #Make sure region code is an accepted one
    acceptedregions = ['01','03','04','05','06','07','08','09','10','12','13',
                       '14','17','18','19','20','21','22','23','24','25']
    if regioncode not in acceptedregions:
        sys.exit("ERROR: Region code " + regioncode + " is not an accepted one.")

    #Make sure labcode is an accepted one
    acceptedlabs = ['SE110', 'SE120', 'SE240', 'SE320', 'SE450', 'SE250', 'SE310', 'SE300', 'SE230',
                    'SE540', 'SE100', 'SE130', 'SE140', 'SE330', 'SE350', 'SE400', 'SE420', 'SE430',
                    'SE440', 'SE600', 'SE610', 'SE620', 'SE700', 'SE710', 'SE720', 'SE730', 'SENPC']
    if labcode not in acceptedlabs:
        sys.exit("ERROR: Lab-code " + labcode + " is not an accepted one.")

    #Check that the logdir is there and accesible
    if not os.path.exists(logdir):
        sys.exit("ERROR: Can not find " + logdir + ". Perhaps you need to create it?")
    if not os.access(logdir, os.W_OK):
        sys.exit("ERROR: No write permissions in " + logdir + ".") 


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
