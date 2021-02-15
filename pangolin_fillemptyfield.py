#!/usr/bin/env python3

import openpyxl
import argparse
import pandas as pd
import os

def arg():
    parser = argparse.ArgumentParser(prog="pangolin_fillemptyfield.py")
    parser.add_argument("-f", "--filepath", help="path to excel file to parse")
    args = parser.parse_args()
    return args


def fill_empty_cells(args):
    df = pd.DataFrame(pd.read_csv(args.filepath, sep="\t")).fillna(value = "NULL")
    df.to_csv(os.path.basename(args.filepath).replace(".txt","_fillempty.txt"), index=None, header=True, sep="\t")

fill_empty_cells(arg())
