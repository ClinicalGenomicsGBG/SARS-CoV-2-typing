#!/usr/bin/env python3

import argparse
import openpyxl
import pandas as pd
import os
import datetime as dt
import fnmatch
import glob
from NGPinterface.hcp import HCPManager
from tools import log 

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


# Check files automatically
@log.log_error("/medstore/logs/pipeline_logfiles/sars-cov-2-typing/direkttestwrapper_cronjob.log")
def check_files():
    now = dt.datetime.now()
    ago = now-dt.timedelta(minutes=1440)

    # Direkttest xlsx files to convert
    xlsx_list = []
    for path in glob.glob('/medstore/results/clinical/SARS-CoV-2-typing/direkttest/direkttest_*.xlsx', recursive=True):
        st = os.stat(path)
        mtime = dt.datetime.fromtimestamp(st.st_ctime)
        if mtime > ago:
            #print('%s modified %s'%(path, mtime))
            xlsx_list.append(path)

    # Direkttest files for upload to HCP
    path_list = []
    for path in glob.glob('/medstore/results/clinical/SARS-CoV-2-typing/direkttest/*', recursive=True):
        st = os.stat(path)
        mtime = dt.datetime.fromtimestamp(st.st_ctime)
        if mtime > ago:
            #print('%s modified %s'%(path, mtime))
            path_list.append(path)
    return xlsx_list,path_list
 

# Convert xlsx to csv and fill empty cells with NULL
@log.log_error("/medstore/logs/pipeline_logfiles/sars-cov-2-typing/direkttestwrapper_cronjob.log")
def csv_from_excel(xlsx_path):
    for x in xlsx_path:
        df = pd.DataFrame(pd.read_excel(x, engine='openpyxl')).fillna(value = "NULL")
        df.to_csv(os.path.abspath(x).replace("xlsx","csv"), index=None, header=True)


# Upload files to HCP
@log.log_error("/medstore/logs/pipeline_logfiles/sars-cov-2-typing/direkttestwrapper_cronjob.log")
def upload_fastq(files_pg, hcpm):
    for file_pg in files_pg:
        if hcpm.upload_file(file_pg, "covid-wgs/"+os.path.basename(file_pg)) is None:
            print(f"uploading: {file_pg}")
        else:
            continue


def main():
    args= arg()

    # Connect to HCP
    hcpm = HCPManager(args.endpoint, args.aws_access_key_id, args.aws_secret_access_key)
    hcpm.attach_bucket(args.bucket)

    xlsx_path, files_pg = check_files()
    csv_from_excel(xlsx_path)
    upload_fastq(files_pg, hcpm)    


if __name__ == "__main__":
    main()
