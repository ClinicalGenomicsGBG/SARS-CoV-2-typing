#!/user/bin/env python

import click
import datetime
import logging
import os
import re
from collections import defaultdict
import csv

@click.command()
@click.option('--logdir', required=True,
              default='/medstore/logs/pipeline_logfiles/sars-cov-2-typing/weekly_report',
              help='Path/to/logfile/directory, uses default if path not specified')
@click.option('--nextseqdir', required=True,
              default='/medstore/results/clinical/SARS-CoV-2-typing/nextseq_data',
              help='Path/to/nextseq_data/directory, uses default if path not specified')
@click.option('--eurofinsdir', required=True,
              default='/medstore/results/clinical/SARS-CoV-2-typing/eurofins_data',
              help='Path/to/eurofind_data/directory, uses default if path not specified')

def main (logdir, nextseqdir, eurofinsdir):
    #Set up the logfile and start loggin
    now = datetime.datetime.now()
    logfile = os.path.join(logdir, "weekly_" + now.strftime("%y%m%d_%H%M%S") + ".log")
    logger = setup_logger('weekly', logfile)
    logger.info('Starting the weekly report workflow.')

    #Find all nextseq runs done
    nextseq_runs = find_nextseqruns(nextseqdir)
    
    logger.info(f'The following NextSeq runs were found: {" ".join(nextseq_runs)}')

    #Find number of samples for nextseq run
    logger.info('Finding number of samples in last weeks runs')
    nextseq_dict = defaultdict(lambda: defaultdict(dict))
    for run in nextseq_runs:
        #Find which week the run belongs to
        rundate = run.split("_")[0]
        runweek = datetime.datetime.strptime(rundate, '%y%m%d').isocalendar()[1]
        
        #Get all the fasta samples
        fastadir = os.path.join(nextseqdir, run, 'fasta')
        #Check if there is a fastadir, if not ignore.
        #This could be due to the pipeline having started 
        #for this sample, but haven't finished
        if os.path.exists(fastadir):
            run_num_samples = num_fasta(fastadir)
            nextseq_dict[runweek][run]['fastas'] = run_num_samples
            if run_num_samples == 0:
                logger.warning(f'Could not find any fasta files for {run}')
        else:
            logger.warning(f'No fasta directory found for run: {run}. Pipeline still not finished?')
            continue

        #Find all pangolin types for nextseq runs
        #First check that the pangolin dir exists
        lineagepath = os.path.join(nextseqdir, run, 'lineage',run + "_lineage_report.txt")
        if os.path.exists(lineagepath):
            nextseq_dict[runweek][run]['lineages'] = pangolin_types(lineagepath)
        else:
            logger.warning(f'No lineage dir found for run: {run}. Pipeline still not finished?')
        
    #Find all eurofins samples which have been sequenced
    #Number of samples
    #Number of all different pangolin types

    #Make an output file (csv + excel)
    #Probably keep appending to the same old file

    #Print header
    print('Week\tRuns\tSequenced Genomes\t', end='')
    #Find all strains sequenced so far
    all_strains = sorted(liststrains(nextseq_dict))
    print("\t".join(all_strains))

    #Print data for all weeks
    for week in nextseq_dict:
        num_runs = len(nextseq_dict[week])
        num_fastas = 0
        for run in nextseq_dict[week]:
            num_fastas += nextseq_dict[week][run]['fastas']

        num_strains = strain_nums(all_strains, nextseq_dict, week)
        print(f'{week}\t{num_runs}\t{num_fastas}', end = '')
        for strain in sorted(num_strains):
            print(f'\t{num_strains[strain]}', end='')
        print("")

def strain_nums(strainlist, nextseq_dict, week):
    #Build a dict with all seen strains
    straindict = {}
    for strain in strainlist:
        straindict[strain] = 0

    #for week in nextseq_dict:
    for run in nextseq_dict[week]:
        for strain in nextseq_dict[week][run]['lineages']:
            straindict[strain] = nextseq_dict[week][run]['lineages'][strain]

    return straindict

def liststrains(nextseq_dict):
    strains = []
    for week in nextseq_dict:
        for run in nextseq_dict[week]:
            for strain in nextseq_dict[week][run]['lineages']:
                if not strain in strains:
                    strains.append(strain)
    return strains


def pangolin_types (lineagepath):
    pango_dict = {}
    #Collect number of each found strain
    with open(lineagepath) as csv_file:
        next(csv_file) #Skip header
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            strain = row[1]
            if strain in pango_dict:
                pango_dict[strain] += 1
            else:
                pango_dict[strain] = 1

    return pango_dict
    
def num_fasta (fastadir):
    num_fasta = 0
    for fasta in os.listdir(fastadir):
        if not fasta.lower().startswith('neg') and not fasta.lower().startswith('pos'):
            num_fasta += 1
    return num_fasta

def find_nextseqruns (nextseqdir):
    dir_list = []
    #Find all runs in folder
    for dirname in next(os.walk(nextseqdir))[1]:
        if re.match('^2[1-4]', dirname):
            dirdate = dirname.split("_")[0]
            dir_list.append(dirname)

            #This is a remnant from when this function only returned runs from last week
            #d = datetime.datetime.strptime(dirdate, "%y%m%d")
            #if ((d - now).days) >= -7:
            #    dir_list.append(dirname)

    return dir_list

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

if __name__ == '__main__':
    main()    
