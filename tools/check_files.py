#!/usr/bin/env python3

import datetime as dt
import glob
import os


# Check files automatically (24hrs)
def check_files(file_path):
    now = dt.datetime.now()
    ago = now-dt.timedelta(minutes=720)

    path_list = []
    for path in glob.glob(file_path, recursive=True):
        st = os.stat(path)
        mtime = dt.datetime.fromtimestamp(st.st_ctime) # ctime for time of change of file
        if mtime > ago:
            #print('%s modified %s'%(path, mtime))
            path_list.append(path)

    return path_list


