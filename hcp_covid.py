#!/usr/bin/env python3

# Wrapper that uploads files.
# Uploads the files to selected bucket on the HCP.
# Lists files in covid-wgs diretory.

import glob
import argparse
import os
import json
import sys
from NGPinterface.hcp import HCPManager

##############################################
# List files that will be uploaded on the HCP.
def files(args):
    file_lst = glob.glob(args.path)
    return file_lst


# Upload files and json to selected bucket on HCP.
def upload_fastq(args, files_pg, hcpm):
#    hcpm = HCPManager(args.endpoint, args.aws_access_key_id, args.aws_secret_access_key)
#    hcpm.attach_bucket(args.bucket)

    # List and upload files provided bu path flag.
    if args.path:
        for file_pg in files_pg:
            hcpm.upload_file(file_pg, args.remotepath+"/"+os.path.basename(file_pg))
            print(f"uploading: {file_pg}")

    if args.filepath:
        # Uploads associated json files.
        hcpm.upload_file(f"{args.filepath}",
                            "covid-wgs"+os.path.basename(args.filepath))

def search(hcpm):
    lst = (hcpm.search_objects("covid-wgs"))
    for i in lst:
        print(i.key)


def arg():
    parser = argparse.ArgumentParser(prog="uploader.py")
    requiredNamed = parser.add_argument_group('required arguments')
    requiredUpload = parser.add_argument_group('additional required arguments for upload')

    requiredUpload.add_argument("-ep", "--endpoint",
                            help="endpoint url")
    requiredUpload.add_argument("-aki", "--aws_access_key_id",
                            help="aws access key id")
    requiredUpload.add_argument("-sak", "--aws_secret_access_key",
                            help="aws secret access key")
    requiredUpload.add_argument("-b", "--bucket",
                            help="bucket name")
    requiredUpload.add_argument("-p", "--path",
                            action='store',
                            help="path to directory with files for upload")
    requiredUpload.add_argument("-f", "--filepath",
                            help="path to single file")
    requiredUpload.add_argument("-l", "--listfiles",
                            action="store_true",
                            help="list existing files")

    args = parser.parse_args()

    return args


def main():
    args = arg()
    files_pg = files(args)

    # Connect to HCP
    hcpm = HCPManager(args.endpoint, args.aws_access_key_id, args.aws_secret_access_key)
    hcpm.attach_bucket(args.bucket)
    
    if args.listfiles:
        search(hcpm)

   upload_fastq(args, files_pg, hcpm)


if __name__ == "__main__":
    main()
