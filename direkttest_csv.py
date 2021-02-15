#!/usr/bin/env python3

import openpyxl
import argparse
import pandas as pd
import os

def arg():
    parser = argparse.ArgumentParser(prog="metadata.py")
    parser.add_argument("-f", "--filepath", help="path to excel file to parse")
    args = parser.parse_args()
    return args


def csv_from_excel(args):
    df = pd.DataFrame(pd.read_excel(args.filepath, engine='openpyxl')).fillna(value = "NULL")
    df.to_csv(os.path.basename(args.filepath).replace("xlsx","csv"), index=None, header=True)

csv_from_excel(arg())
