#!/usr/bin/env python

import click
import datetime
import os
import sys
import glob
from sample_sheet import SampleSheet
from collections import defaultdict
import pysftp
import time

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
@click.option('--sshkey', required=True,
              help='Path to SSH key to use for sFTP connection')
@click.option('-l', '--logdir', required=True,
              default='/medstore/logs/pipeline_logfiles/sars-cov-2-typing/GENSAM-upload',
              help='Path to directory where logs should be created')
@click.option('--sftpusername', required=True,
              help='Username to the GENSAM sFTP')
@click.option('--sftppassword', required=True,
              help='Password to the GENSAM sFTP')
def main(runid, demultiplexdir, logdir, inputdir, samplesheetname, regioncode, labcode, sshkey, sftpusername, sftppassword):
    #Get the path to samplesheet
    sspath = os.path.join(demultiplexdir, runid, samplesheetname)

    #Run checks on all given inputs
    checkinput(runid, demultiplexdir, inputdir, regioncode, labcode, logdir, samplesheetname, sspath)
    
    # Start the logging
    now = datetime.datetime.now()
    logfile = os.path.join(logdir, "GENSAM-upload_" + now.strftime("%y%m%d_%H%M%S") + ".log")
    logfile_sftp = os.path.join(logdir, "GENSAM-upload_" + now.strftime("%y%m%d_%H%M%S") + "_sFTP.log")
    logfile = os.path.join(logdir, "GENSAM-upload_debug.log")
    logfile_sftp = os.path.join(logdir, "GENSAM-upload_debug_sftp.log")


    log = open(logfile, "a")
    log.write("----\n")
    log.write(writelog("LOG", "Starting GENSAM upload workflow"))
 
    #Read in all sampleIDs
    samples = sample_sheet(sspath)

    #Get a list of all fastq and fasta files to upload
    syncdict = defaultdict(lambda: defaultdict(dict))
    log.write(writelog("LOG", "Finding all files to upload."))
    
    for sample in samples:
        #Find all fastq files to upload. Also count them
        fastqpath = os.path.join(inputdir, runid, 'fastq')
        for fastqfile in glob.glob(fastqpath + "/" + sample + "*fastq.gz"):
            targetlink = os.readlink(fastqfile)
            if targetlink.endswith("R1_001.fastq.gz"): #Fastq files need to have this extension right now
                syncdict[sample]['fastq']['R1'] = targetlink
            elif targetlink.endswith("R2_001.fastq.gz"):
                syncdict[sample]['fastq']['R2'] = targetlink
            else:
                log.write(writelog("ERROR", "Found fastq file with ending other than R1(R2)_001.fastq.gz"))
                sys.exit("ERROR: Found fastq file with ending other than R1(R2)_001.fastq.gz")
        
        #Find all fasta files to upload based on existing links. Also count them
        fastapath = os.path.join(inputdir, runid, 'fasta')
        for fastafile in glob.glob(fastapath + "/" + sample + "*consensus.fa"):
            targetlink = os.readlink(fastafile)
            #Store info in dict
            syncdict[sample]['fasta'] = targetlink

    #Check that all fastq files are paired
    #This code feels a bit clunky
    for sample in syncdict:
        if syncdict[sample]['fastq']['R1']:
            if not syncdict[sample]['fastq']['R2']:
                log.write(writelog("ERROR", "No R2 file found for " + syncdict[sample]['fastq']['R1'] + "."))
                sys.exit("ERROR: No R2 file found for " + syncdict[sample]['fastq']['R1'] + ".")
        if syncdict[sample]['fastq']['R2']:
            if not syncdict[sample]['fastq']['R1']:
                log.write(writelog("ERROR", "No R1 file found for " + syncdict[sample]['fastq']['R2'] + "."))
                sys.exit("ERROR: No R1 file found for " + syncdict[sample]['fastq']['R2'] + ".")

    #Check how manny files there is to upload
    for filetype in ['fastq', 'fasta']:
        numfiles = countkeys(syncdict, filetype)
        if filetype == 'fastq':
            log.write(writelog("LOG", "Found " + str(numfiles) + " " + filetype  + " pairs to upload."))
        else:
            log.write(writelog("LOG", "Found " + str(numfiles) + " " + filetype  + " files to upload."))

    #Open the connection to the sFTP
    log.write(writelog("LOG", "Starting sFTP upload."))
    try:
        sftp = pysftp.Connection('seqstore.sahlgrenska.gu.se', username=sftpusername, password=sftppassword, log=logfile_sftp)
        sftp.chdir("shared/test_gensam")
    except:
        log.write(writelog("ERROR", "Establishing sFTP connection failed. Check the sFTP log @ " + logfile_sftp))
        sys.exit("ERROR: Establishing sFTP connection failed. Check the sFTP log @ " + logfile_sftp)

    #Upload all files to the FOHM FTP
    #fastq and fasta file
    for sample in syncdict:
        #Get all fastq files and construct correct names
        if syncdict[sample]['fastq']['R1']: #Check if sample has fastq files to upload
            fastqR1_src = syncdict[sample]['fastq']['R1']
            fastqR2_src = syncdict[sample]['fastq']['R2']

            samplename_R1 = sample.replace("_", "-") + '_1.fastq.gz'
            fastqR1_trgt = '_'.join((regioncode, labcode, samplename_R1))
            samplename_R2 = sample.replace("_", "-") + '_2.fastq.gz'
            fastqR2_trgt = '_'.join((regioncode, labcode, samplename_R2))
            #Upload to sFTP
            sftp.put(fastqR1_src, fastqR1_trgt)
            sftp.put(fastqR2_src, fastqR2_trgt)
            
        # Get all fasta files and construct correct names
        if syncdict[sample]['fasta']: #Check if sample has fastq files to upload
            fasta_src = syncdict[sample]['fasta']
            samplename_fasta = sample.replace("_", "-") + '.consensus.fasta'
            fasta_trgt = '_'.join((regioncode, labcode, samplename_fasta))
            #Upload to sFTP
            sftp.put(fasta_src, fasta_trgt)


    #Pangolin classification lineage file
    pango_now = datetime.datetime.now()
    pango_date = pango_now.strftime("%Y-%m-%d")
    
    lineagepath = os.path.join(inputdir, runid, 'lineage', runid + "_lineage_report_gensam.txt")
    lineage_trgt = '_'.join((regioncode, labcode, pango_date, "pangolin_classification.txt"))
    sftp.put(lineagepath, lineage_trgt)

    #Close the sFTP connection
    sftp.close()
    log.write(writelog("LOG", "Finished the sFTP upload."))

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
        #Specifically check if the lineage file is in place
        if not os.path.exists(os.path.join(inputdir, runid, 'lineage', runid + "_lineage_report.txt")):
            sys.exit("ERROR: No lineage file for run " + runid + " found.")

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

def countkeys(dictname, group):
    count = 0
    for keys in dictname:
        if dictname[keys][group]:
            count += 1
    return count
       
if __name__ == '__main__':
    main()
