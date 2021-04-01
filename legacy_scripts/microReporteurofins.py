#!/usr/bin/env python

import datetime
import os
import click
import glob
from shutil import copy

@click.command()
@click.option('-e', '--eurofinsdir', required=True,
              default='/medstore/results/clinical/SARS-CoV-2-typing/eurofins_data/goteborg',
              help='Path to Eurofins data download directory')
@click.option('-s', '--syncdir', required=True,
              default='/seqstore/remote/outbox/sarscov2-micro/shared/eurofins',
              help='Path to sFTP directory to sync to')
@click.option('-f', '--syncedfiles', required=True,
              default='/apps/bio/repos/sars-cov-2-typing/eurofins-dl/syncedFiles.txt',
              help='Path to file which keeps track of what has already been copied over')
@click.option('-l', '--logfile', required=True,
              default='/medstore/logs/pipeline_logfiles/sars-cov-2-typing/microReport.log',
              help='Path to log file')
def main(eurofinsdir, syncdir, syncedfiles, logfile):
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
    for pangolin_file in pangolin_list:
        pangolin_base = os.path.basename(pangolin_file)
        # Have the file already been copied over at some point?
        if pangolin_base not in synced:
            log.write("** LOG: Copying " + pangolin_base + " to " + syncdir + ".\n")
            copy(pangolin_file, syncdir)
            f.write(pangolin_base + "\n")
        else:
            continue

    f.close()

    # Write an end to the log
    now = datetime.datetime.now()
    log.write("** LOG: Finished copying @ " + now.strftime("%Y-%m-%d %H:%M:%S") + "\n")
    log.write("---\n")
    log.close()

if __name__ == '__main__':
    main()

# Kolla vilka filer som redan laddats over - Lasa in fran en fil
# Hitta alla pangolin resultat i eurofins mappen
# Synca over de som saknas
# Gora en md5sum pa filen nar den ar pa plats
