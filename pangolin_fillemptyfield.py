#!/usr/bin/env python3

import argparse
import pandas as pd
import os
import datetime as dt
import fnmatch
import glob

def arg():
    parser = argparse.ArgumentParser(prog="pangolin_fillemptyfield.py")
    parser.add_argument("-f", "--filepath", help="path to excel file to parse")
    parser.add_argument("-a", "--automatic", action="store_true", help="check for files automatically")
    args = parser.parse_args()
    return args

def check_files():
    now = dt.datetime.now()
    ago = now-dt.timedelta(minutes=1440)
#print(os.path.basename(os.path.dirname(path)))

    path_list = []
    #for path in glob.glob('/medstore/results/clinical/SARS-CoV-2-typing/eurofins_data/goteborg/2021*/*', recursive=True):
    for path in glob.glob('/home/xcanfv/GBG_sars-cov-2-typing/sars-cov-2-typing/*', recursive=True):
        st = os.stat(path)
        mtime = dt.datetime.fromtimestamp(st.st_mtime)
        if mtime > ago:
            #print('%s modified %s'%(path, mtime))
            path_list.append(path)
    return path_list


def automatic(path):
    for f in path:
        if fnmatch.fnmatch(os.path.basename(f), "*_pangolin_lineage_classification.txt"):
            print("updating: " + f)
            df = pd.DataFrame(pd.read_csv(f, sep="\t")).fillna(value = "NULL")
            df.to_csv(os.path.basename(os.path.dirname(f))+"_"+os.path.basename(f).replace(".txt","_fillempty.txt"), index=None, header=True, sep="\t")


def fill_empty_cells(args):
    df = pd.DataFrame(pd.read_csv(args.filepath, sep="\t")).fillna(value = "NULL")
    df.to_csv(os.path.basename(args.filepath).replace(".txt","_fillempty.txt"), index=None, header=True, sep="\t")


def main():
    args = arg()
    if args.automatic:
        path = check_files()
        automatic(path)

    else:
        fill_empty_cells(args)


if __name__ == "__main__":
    main()
