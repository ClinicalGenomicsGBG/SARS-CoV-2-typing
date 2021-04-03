#!/usr/bin/env python

import datetime
import os
import click
import glob
from shutil import copy

def eurofins(eurofinsdir, syncdir, syncedfiles, logfile):
    log = open(logfile, "a")
    now = datetime.datetime.now()
    log.write("** LOG: Starting sync of pangolin files to microbiology outbox @ "
              + now.strftime("%Y-%m-%d %H:%M:%S") + "\n")

    # Read in already copied files
    with open(syncedfiles) as sync_file:
        synced = sync_file.read().splitlines()

    # Find all pangolin files in the Eurofins folder
    pangolin_list = glob.glob(eurofinsdir + "/*/*_pangolin_lineage_classification_fillempty.txt")

    # Add the synced files to the list
    f = open(syncedfiles, "a")
    synclist = []
    for pangolin_file in pangolin_list:
        pangolin_base = os.path.basename(pangolin_file)
        # Have the file already been copied over at some point?
        if pangolin_base not in synced:
            #Skip if it is already in the syncdir
            if os.path.exists(os.path.join(syncdir, pangolin_base)):
                log.write(f'** LOG: {pangolin_base} already in {syncdir}. Skipping.')
                try:
                    f.write(pangolin_base + "\n")
                except:
                    log.write(f'** ERROR: Could not write {pangolin_base} to {syncedfiles}.')
            else:
                log.write("** LOG: Copying " + pangolin_base + " to " + syncdir + ".\n")
                try:
                    copy(pangolin_file, syncdir)
                except:
                    log.write(f'** ERROR: Could not copy {pangolin_base}.\n')
                try:
                    f.write(pangolin_base + "\n")
                    synclist.append(pangolin_base)
                except: 
                    log.write(f'** ERROR: Could not write {pangolin_base} to {syncedfiles}.')

        else:
            continue

    #Closing the syncfiles file
    log.write(f'** LOG: Closing {syncedfiles}')
    try:
        f.close()
    except:
        log.write(f'** ERROR: Could not close {syncedfiles}.')

    # Write an end to the log
    now = datetime.datetime.now()
    log.write("** LOG: Finished copying @ " + now.strftime("%Y-%m-%d %H:%M:%S") + "\n")
    log.write("---\n")
    log.close()

    return synclist
def nextseq(nextseqdir, articdir, syncdir, syncedfiles, logfile):
    log = open(logfile, "a")
    now = datetime.datetime.now()
    log.write("** LOG: Starting sync of pangolin files to microbiology outbox @ "
              + now.strftime("%Y-%m-%d %H:%M:%S") + "\n")

    # Read in already copied files
    with open(syncedfiles) as sync_file:
        synced = sync_file.read().splitlines()

    # Find all pangolin files in the Eurofins folder
    pangolin_list = glob.glob(nextseqdir + "/*/lineage/*_lineage_report_fillempty.txt")

    # Find all pangolin files in the Eurofins folder
    artic_list = glob.glob(articdir + "/*/*.qc.csv")

    # Add the synced files to the list
    f = open(syncedfiles, "a")
    synclist = []
    for pangolin_file in pangolin_list:
        pangolin_base = os.path.basename(pangolin_file)
        # Have the file already been copied over at some point?
        if pangolin_base not in synced:
            log.write("** LOG: Copying " + pangolin_base + " to " + syncdir + ".\n")
            copy(pangolin_file, syncdir)
            f.write(pangolin_base + "\n")
            synclist.append(pangolin_base)
        else:
            continue

    f.close()

    # Add the synced artic files to the list
    fa = open(syncedfiles, "a")
    for artic_file in artic_list:
        artic_base = os.path.basename(artic_file)
        # Have the file already been copied over at some point?
        if artic_base not in synced:
            log.write("** LOG: Copying " + artic_base + " to " + syncdir + ".\n")
            copy(artic_file, syncdir)
            fa.write(artic_base + "\n")
        else:
            continue

    fa.close()
    
    # Write an end to the log
    now = datetime.datetime.now()
    log.write("** LOG: Finished copying @ " + now.strftime("%Y-%m-%d %H:%M:%S") + "\n")
    log.write("---\n")
    log.close()

    return synclist

if __name__ == '__main__':
    main()

# Kolla vilka filer som redan laddats over - Lasa in fran en fil
# Hitta alla pangolin resultat i eurofins mappen
# Synca over de som saknas
# Gora en md5sum pa filen nar den ar pa plats
