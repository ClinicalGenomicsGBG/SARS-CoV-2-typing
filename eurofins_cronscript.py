#!/usr/bin/env python3

import argparse
import os
import pandas as pd
import fnmatch
import logging
import datetime
from NGPinterface.hcp import HCPManager
from tools import log
from tools.check_files import check_files
from tools.microReport import eurofins as microreport
from tools.syncsftp import main as syncsftp
from tools.emailer import email_micro

def arg():
    parser = argparse.ArgumentParser(prog="direkttest_cronscript.py")
    requiredNamed = parser.add_argument_group('required arguments')

    requiredNamed.add_argument("-ep", "--endpoint",
                            help="endpoint url")
    requiredNamed.add_argument("-aki", "--aws_access_key_id",
                            help="aws access key id")
    requiredNamed.add_argument("-sak", "--aws_secret_access_key",
                            help="aws secret access key")
    requiredNamed.add_argument("-b", "--bucket",
                            help="bucket name")
    requiredNamed.add_argument("-u", "--username",
                            help="username for eurofins sftp connection")
    requiredNamed.add_argument("-p", "--password",
                            help="password for eurofins sftp connection")
    args = parser.parse_args()

    return args


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


@log.log_error("/medstore/logs/pipeline_logfiles/sars-cov-2-typing/eurofinswrapper_cronjob.log")
# Sync eurofins data
def sync_sftp(args):
    logdir = "/medstore/logs/pipeline_logfiles/sars-cov-2-typing/eurofins-sftp"
    dataloc = "/medstore/results/clinical/SARS-CoV-2-typing/eurofins_data/goteborg"
    eurofinshost = "ftp.gatc-biotech.com"
    username = args.username
    password = args.password
    syncsftp(logdir, dataloc, eurofinshost, username, password, no_mail=False, no_sync=False)


@log.log_error("/medstore/logs/pipeline_logfiles/sars-cov-2-typing/eurofinswrapper_cronjob.log")
# Fix pangolin by filling empty fields with NULL
def pangolin(pangolin_path):
    for f in pangolin_path:
        if fnmatch.fnmatch(os.path.basename(f), "*_pangolin_lineage_classification.txt"): 
            print("updating: " + f)
            df = pd.DataFrame(pd.read_csv(f, sep="\t")).fillna(value = "NULL")
            df.to_csv(os.path.abspath(f).replace(".txt","_fillempty.txt"), index=None, header=True, sep="\t") 


@log.log_error("/medstore/logs/pipeline_logfiles/sars-cov-2-typing/eurofinswrapper_cronjob.log")
# Sync pangolin files to micro sftp
def micro_report():
    eurofinsdir = "/medstore/results/clinical/SARS-CoV-2-typing/eurofins_data/goteborg"
    syncdir = "/seqstore/remote/outbox/sarscov2-micro/shared/eurofins"
    syncedfiles = "/medstore/results/clinical/SARS-CoV-2-typing/microbiologySync/syncedFiles.txt"
    logfile = "/medstore/logs/pipeline_logfiles/sars-cov-2-typing/microReport.log"
    microreport(eurofinsdir, syncdir, syncedfiles, logfile)

    message_subject = "New Eurofins pangolin files @ sFTP"
    message_body = "New pangolin files from Eurofins are now available at the KMIK sFTP."
    email_micro(message_subject, message_body)

# Upload files and json to selected bucket on HCP.
def upload_fastq(hcp_paths,hcpm,logger):
    for file_pg in hcp_paths:
        if "md5sums.txt" in file_pg or file_pg.endswith("classification.txt"):
            continue
        else:
            try:
                 hcpm.upload_file(file_pg, "covid-wgs/"+os.path.basename(file_pg))
                 logger.info(f"uploading: {file_pg}")
            except Exception as e:
                logger.error(e)
                continue
    
def main():
    args = arg()

    #Set up the logfile
    now = datetime.datetime.now()
    logfile = os.path.join("/medstore/logs/pipeline_logfiles/sars-cov-2-typing/HCP_upload/", "HCP_upload_eurofins" + now.strftime("%y%m%d_%H%M%S") + ".log")
    logger = setup_logger('hcp_log', logfile)

    # Connect to HCP
    hcpm = HCPManager(args.endpoint, args.aws_access_key_id, args.aws_secret_access_key)
    hcpm.attach_bucket(args.bucket)

    # Mirror files from eurofins
    sync_sftp(args)
    
    # Find panoling files and add NULL to empty fields
    pangolin_path = check_files("/medstore/results/clinical/SARS-CoV-2-typing/eurofins_data/goteborg/2*/*_pangolin_lineage_classification.txt")
    pangolin(pangolin_path)

    # Find pangolin files and sync to micro
    micro_report()

    # Find eurofins files and upload to HCP
    hcp_paths = check_files("/medstore/results/clinical/SARS-CoV-2-typing/eurofins_data/goteborg/2021*/*")
    upload_fastq(hcp_paths,hcpm,logger)


if __name__ == "__main__":
    main()
