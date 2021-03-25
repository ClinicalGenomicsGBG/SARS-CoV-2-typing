#!/usr/bin/env python3

import argparse
import openpyxl
import pandas as pd
import os
import fnmatch
import glob
import datetime
import logging
from NGPinterface.hcp import HCPManager
from tools import log 
from tools.check_files import check_files
from tools.direkttest_csv import csv_from_excel as csv_parse

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


# Convert xlsx to csv and fill empty cells with NULL
@log.log_error("/medstore/logs/pipeline_logfiles/sars-cov-2-typing/direkttestwrapper_cronjob.log")
def csv_from_excel(xlsx_path):
    for x in xlsx_path:
        csv_parse(x)


# Upload files to HCP
@log.log_error("/medstore/logs/pipeline_logfiles/sars-cov-2-typing/direkttestwrapper_cronjob.log")
def upload_fastq(files_pg, hcpm, logger):
    for file_pg in files_pg:
        try:
            hcpm.upload_file(file_pg, "covid-wgs/"+os.path.basename(file_pg))
            logger.info(f"uploading: {file_pg}")
        except Exception as e:
            logger.error(e)
            continue
        #if hcpm.upload_file(file_pg, "covid-wgs/"+os.path.basename(file_pg)) is None:
        #    print(f"uploading: {file_pg}")
           
        #else:
        #    continue


def main():
    args= arg()

    #Set up the logfile
    now = datetime.datetime.now()
    logfile = os.path.join("/medstore/logs/pipeline_logfiles/sars-cov-2-typing/HCP_upload/", "HCP_upload_direkttest" + now.strftime("%y%m%d_%H%M%S") + ".log")
    logger = setup_logger('hcp_log', logfile)

    # Connect to HCP
    hcpm = HCPManager(args.endpoint, args.aws_access_key_id, args.aws_secret_access_key)
    hcpm.attach_bucket(args.bucket)

    # Find files using check_files module
    # Convert xlsx files and upload to HCP
    xlsx_path = check_files("/medstore/results/clinical/SARS-CoV-2-typing/direkttest/direkttest_*.xlsx")
    csv_from_excel(xlsx_path)

    files_pg = check_files("/medstore/results/clinical/SARS-CoV-2-typing/direkttest/*")
    upload_fastq(files_pg,hcpm,logger)    


if __name__ == "__main__":
    main()
