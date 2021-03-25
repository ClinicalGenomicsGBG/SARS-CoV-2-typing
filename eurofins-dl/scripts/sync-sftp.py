#!/usr/bin/env python

import click
import datetime
import os
import sys
import logging
import smtplib
from email.message import EmailMessage
import subprocess
import glob

@click.command()
@click.option('--logdir', required=True,
              default='/medstore/logs/pipeline_logfiles/sars-cov-2-typing/eurofins-sftp',
              help='Path/to/logfile/directory')
@click.option('--dataloc', required=True,
              default='/medstore/results/clinical/SARS-CoV-2-typing/eurofins_data/goteborg',
              help='Path/to/data-storage/directory')
@click.option('--eurofinshost', required=True,
              default='ftp.gatc-biotech.com',
              help='Eurofins FTP hostname')
@click.option('-u', '--username', required=True,
              help='Eurofins FTP username')
@click.option('-p', '--password', required=True,
              help='Eurofins FTP password')
@click.option('--no-mail', is_flag=True,
              help="Set if you do NOT want e-mails to be sent")
@click.option('--no-sync', is_flag=True,
              help="Set if you do NOT want to sync files, just test the FTP connection")

def main (logdir, dataloc, eurofinshost, username, password, no_mail, no_sync):
    #Run checks on all given inputs
    checkinput(logdir, dataloc)

    #Set up the logfile
    now = datetime.datetime.now()
    logfile = os.path.join(logdir, "Eurofins-SFTP-sync_" + now.strftime("%y%m%d_%H%M%S") + ".log")
    logger = setup_logger('sftp_sync', logfile)

    #Start the workflow
    logger.info('Starting the FTP sync workflow.')

    #Sync the FTP
    if no_sync:
        logger.info('No-sync flag set. Testing the FTP connection')
        lftpcommand = ['lftp', '-u', f'{username},{password}', f'{eurofinshost}:21',
                   "-e", f'echo "Testing connection.";bye']
    else:
        logger.info(f'Starting syncing of FTP folders to {dataloc}') 
        lftpcommand = ['lftp', '-u', f'{username},{password}', f'{eurofinshost}:21',
                   "-e", f'mirror -vv / {dataloc};bye']

    try:
        lftpresult = subprocess.run(lftpcommand)
        lftpresult.check_returncode()
        if no_sync:
            logger.info('FTP connection OK.')
        else:
            logger.info('Completed FTP sync.')
    except:
        logger.error('FTP sync failed.')
        if not no_mail:
            email_error(logfile, "FTP SYNC")
        sys.exit('ERROR: FTP sync failed.')

    #Check the new md5sums which was downloaded
    md5files = get_md5files(now, dataloc)

    for md5file in md5files:
        md5path = os.path.dirname(md5file)
        md5dir = os.path.basename(os.path.normpath(md5path))

        md5command = ["md5sum", "-c", "--quiet", md5file]
        logger.info(f'Checking MD5 sums for downloaded files in {md5dir}.')
        try:
            md5results = subprocess.run(md5command, cwd=md5path)
            md5results.check_returncode()
            logger.info(f'All MD5 sums correct for files in {md5dir}.')
        except:
            logger.error(f'Incorrect MD5 sum found in {md5file}.')
            if not no_mail:
                email_error(logfile, "MD5 SUM CHECK")
            sys.exit(f'ERROR: Incorrect md5 sum found in {md5file}.')

    # Finish workflow
    logger.info('Finished the FTP sync workflow')
    
def checkinput(logdir, dataloc):
    #Check that the logdir is there and accesible
    if not os.path.exists(logdir):
        sys.exit("ERROR: Can not find " + logdir + ". Perhaps you need to create it?")
    if not os.access(logdir, os.W_OK):
        sys.exit("ERROR: No write permissions in " + logdir + ".") 

    #Check that the datalocation to write files to exist and has premissions
    if not os.path.exists(dataloc):
        sys.exit("ERROR: Can not find " + dataloc + ". Perhaps you need to create it?")
    if not os.access(logdir, os.W_OK):
        sys.exit("ERROR: No write permissions in " + dataloc + ".")

def get_md5files(now, dataloc):
    ago = now-datetime.timedelta(minutes=720)
    path_list = []
    for path in glob.glob(f'{dataloc}/*/md5sums.txt', recursive=True):
        st = os.stat(path)
        mtime = datetime.datetime.fromtimestamp(st.st_ctime)
        if mtime > ago:
            path_list.append(path)
    return path_list
    

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
    msg.set_content("Errors were encountered during the automatic sync of the Eurofins sFTP.\n\n" +
    "The error occured during: " + errorstep + "\n"
    "Please check the log file @  " + logloc)

    msg['Subject'] = "ERROR: Eurofins sFTP sync"
    msg['From'] = "clinicalgenomics@gu.se"
    msg['To'] = "clinicalgenomics@gu.se"

    #Send the messege
    s = smtplib.SMTP('smtp.gu.se')
    s.send_message(msg)
    s.quit()

if __name__ == '__main__':
    main()
