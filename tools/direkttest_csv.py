#!/usr/bin/env python3

import openpyxl
import argparse
import pandas as pd
import os
import datetime as dt
import fnmatch
import glob

def arg():
    parser = argparse.ArgumentParser(prog="direkttest_csv.py")
    parser.add_argument("-f", "--filepath", help="path to excel file to parse")
    parser.add_argument("-a", "--automatic", action="store_true", help="automatic file search from direkttest")

    args = parser.parse_args()
    return args


def check_files():
    now = dt.datetime.now()
    ago = now-dt.timedelta(minutes=1440)
#print(os.path.basename(os.path.dirname(path)))

    path_list = []
    for path in glob.glob('/medstore/results/clinical/SARS-CoV-2-typing/direkttest/direkttest_*.xlsx', recursive=True):
        st = os.stat(path)
        mtime = dt.datetime.fromtimestamp(st.st_ctime)
        if mtime > ago:
            #print('%s modified %s'%(path, mtime))
            path_list.append(path)
    return path_list


def csv_from_excel(path):
    df = pd.DataFrame(pd.read_excel(path, engine='openpyxl')).fillna(value = "NULL")
    df.to_csv(os.path.abspath(path).replace("xlsx","csv"), index=None, header=True)


def main():
    args = arg()
    if args.automatic:
        path = check_files()
        for p in path:
            csv_from_excel(p)

    else:
        path = args.filepath
        csv_from_excel(path)


if __name__ == "__main__":
    main()
