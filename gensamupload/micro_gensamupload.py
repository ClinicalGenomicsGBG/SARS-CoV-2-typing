#!/usr/bin/env python

import click
import datetime
import glob
import os
import sys
from shutil import copyfile
import smtplib
from email.message import EmailMessage
import logging
from collections import defaultdict

@click.command()
@click.option('-d', '--datadir', required=True,
              default='/seqstore/remote/inbox/micro-gensam/shared',
              help='Path to micro sFTP inbox')
@click.option('-l', '--logdir', required=True,
              default='/medstore/logs/pipeline_logfiles/sars-cov-2-typing/micro-GENSAM-upload',
              help='Path dir where log should be saved')
@click.option('--regioncode', required=True,
              default='14',
              help='FOHM region code. Default is 14')
@click.option('--labcode', required=True,
              default='SE300',
              help='FOHM lab code. Default is SE300')
@click.option('--no-mail', is_flag=True,
              help="Set if you do NOT want e-mails to be sent")
def main (datadir, logdir, regioncode, labcode, no_mail):

    #Run checks on all given inputs
    checkinput(datadir, logdir, regioncode, labcode)

    #Set up the logfile
    now = datetime.datetime.now()
    logfile = os.path.join(logdir, "micro-GENSAM-upload_" + now.strftime("%y%m%d_%H%M%S") + ".log")
    logger = setup_logger('micro_gensam', logfile)

    #Start the workflow
    logger.info("Starting the microbiology GENSAM-upload workflow.")

    #Find all the relevant files in the datadir. Makes sure everything is there
    try:
        syncfiles = collect_files(datadir, regioncode, labcode)
    except Exception as e:
        logger.error(e)
        sys.exit()

    #Go over the csv file and make sure there are fastq, fasta and variants for each sample
    
        
    for syncfile in syncfiles.keys():
        print(syncfile)
        for item in syncfiles[syncfile]:
            print(item)
            

def collect_files(datadir, regioncode, labcode):
    filedict = defaultdict(list)
    for syncfile in glob.glob(datadir +"/*"):
        #Check each kind of file independently
        correctstart = f'{regioncode}_{labcode}_' #Same for all files        
        #All files should have same prefix
        if not os.path.basename(syncfile).startswith(correctstart):
                raise Exception(f'Found file, {os.path.basename(syncfile)}, not starting with \'{correctstart}\'')
        
        #FASTA files
        if syncfile.endswith('.fasta'):
            #Check that it has the correct name
            correctfastaend = 'consensus.fasta'
            if not syncfile.endswith(correctfastaend):
                raise Exception(f'Found fasta file, {os.path.basename(syncfile)}, not ending with \'{correctfastaend}\'.')
            else:
                filedict['fasta'].append(syncfile) 

        #FASTQ filess
        elif syncfile.endswith('fastq.gz'):
            filedict['fastq'].append(syncfile)

        #Variant file
        elif syncfile.endswith('.vcf'):
            filedict['variants'].append(syncfile)

        #Pangolin classification file
        elif syncfile.endswith('.txt'):
            correctpangolinend = '_pangolin_classification.txt'
            if not syncfile.endswith(correctpangolinend):
                raise Exception(f'Found .txt file, {os.path.basename(syncfile)}, not ending with \'{correctpangolinend}\'.')
            else:
                filedict['pangolin'].append(syncfile)

        elif syncfile.endswith('.csv'):
            correctcsvend = '_komplettering.csv'
            if not syncfile.endswith(correctcsvend):
                raise Exception(f'Found .csv file, {os.path.basename(syncfile)}, not ending with \'{correctcsvend}\'.')
            else:
                filedict['csv'].append(syncfile)

    return filedict


def checkinput(datadir, logdir, regioncode, labcode):
    #Check that the datalocation to write files to exist and has premissions
    if not os.path.exists(datadir):
        sys.exit("ERROR: Can not find " + datadir + ". Perhaps you need to create it?")
    if not os.access(logdir, os.R_OK):
        sys.exit("ERROR: No read permissions in " + datadir + ".")
    
    #Check that the logdir is there and accesible
    if not os.path.exists(logdir):
        sys.exit("ERROR: Can not find " + logdir + ". Perhaps you need to create it?")
    if not os.access(logdir, os.W_OK):
        sys.exit("ERROR: No write permissions in " + logdir + ".")

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

def setup_logger(name, log_path=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    stream_handle = logging.StreamHandler()
    stream_handle.setLevel(logging.DEBUG)
    stream_handle.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(stream_handle)

    if log_path:
        file_handle = logging.FileHandler(log_path, 'a')
        file_handle.setLevel(logging.DEBUG)
        file_handle.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handle)

    return logger

def email_error(logloc, errorstep):
    msg = EmailMessage()
    msg.set_content("Errors were encountered during the automatic upload of microbiology files to FOHM GENSAM.\n\n" +
    "The error occured during: " + errorstep + "\n"
    "Please check the log file @  " + logloc)

    msg['Subject'] = "ERROR: Microbiology GENSAM upload"
    msg['From'] = "clinicalgenomics@gu.se"
    msg['To'] = "anders.lind.cggs@gu.se"

    #Send the messege
    s = smtplib.SMTP('smtp.gu.se')
    s.send_message(msg)
    s.quit()

if __name__ == '__main__':
    main()
