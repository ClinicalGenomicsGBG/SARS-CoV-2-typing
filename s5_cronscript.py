import os
import re
import csv
import json
import shutil
import pysftp
import logging
import subprocess
import logging.handlers

import click

from Bio import SeqIO
from datetime import datetime

from tools.lookup import selection_criteria
from tools.emailer import email_fohm

# Wrapper specific
DATADUMP_ROOT = '/seqstore/instruments/s5_01_mik/covid_datadump'

LOG_PATH = '/medstore/logs/pipeline_logfiles/sars-cov-2-typing/micro-GENSAM-upload/s5_cronscript'
UPLOAD_PATH = '/medstore/results/clinical/SARS-CoV-2-typing/microbiology_data/iontorrent_gensam'
UPLOADED_SAMPLE_LIST_PATH = os.path.join(UPLOAD_PATH, 'previous_sample_uploads.txt')
UPLOADED_RUN_LIST_PATH = os.path.join(UPLOAD_PATH, 'previous_run_uploads.txt')

GENSAM_URL = 'gensam-sftp.folkhalsomyndigheten.se'
GENSAM_USER = 'se300'
GENSAM_SSH_KEY = '/home/cronuser/.ssh/id_rsa'
GENSAM_SSH_KEY_PASSWORD = ''
GENSAM_URL_SUBDIRECTORY = 'till-fohm'
REGION_CODE = '14'
LAB_CODE = 'SE300'


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

stream_handle = logging.StreamHandler()
stream_handle.setLevel(logging.DEBUG)
stream_handle.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handle)


file_handle = logging.handlers.TimedRotatingFileHandler(LOG_PATH, when='W6')  # Midnight sunday-monday
file_handle.namer = lambda x: x.replace(".log", "") + ".log"
file_handle.setLevel(logging.DEBUG)
file_handle.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handle)


class IncompleteTransferError(Exception):
    """Raise on attempting work on incomplete run transfer."""


class SampleContext:
    def __init__(self, sample_name, input_path):
        self.sample_name = sample_name
        self.input_path = input_path

        # From data dump of plugin
        self.fastq_path = os.path.join(self.input_path, f'{sample_name}.fastq.gz')
        self.vcf_path = os.path.join(self.input_path, f'{sample_name}.vcf.gz')
        self.fasta_path = os.path.join(self.input_path, f'{sample_name}.fa')

        # Proper GENSAM names
        self.gensam_basename = f'{REGION_CODE}_{LAB_CODE}_{self.sample_name}'
        self.gensam_fastq_name = f'{self.gensam_basename}.fastq.gz'
        self.gensam_vcf_name = f'{self.gensam_basename}.vcf.gz'  # NOTE: Gunzipped to without .gz
        self.gensam_fasta_name = f'{self.gensam_basename}.consensus.fasta'

    def has_all_files(self):
        fastq_exist = os.path.exists(self.fastq_path)
        vcf_exist = os.path.exists(self.vcf_path)
        fasta_exist = os.path.exists(self.fasta_path)

        return fastq_exist and vcf_exist and fasta_exist

    def reformat_fasta(self, output_directory_path):
        """Set the fasta header to the GENSAM basename and print to correctly named file."""
        gensam_fasta_output_path = os.path.join(output_directory_path, self.gensam_fasta_name)
        with open(gensam_fasta_output_path, 'w') as out:
            for record in SeqIO.parse(self.fasta_path, 'fasta'):
                record.id = self.gensam_basename
                record.description = ''
                SeqIO.write(record, out, 'fasta')
        return gensam_fasta_output_path

    def reformat_vcf(self, output_directory_path):
        """Copy vcf to correctly named file."""
        gensam_vcf_output_path = os.path.join(output_directory_path, self.gensam_vcf_name)
        shutil.copyfile(self.vcf_path, gensam_vcf_output_path)
        subprocess.run(['gunzip', gensam_vcf_output_path])
        return gensam_vcf_output_path.replace('.gz', '')

    def reformat_fastq(self, output_directory_path):
        """Copy fastq to correctly named file."""
        gensam_fastq_output_path = os.path.join(output_directory_path, self.gensam_fastq_name)
        shutil.copyfile(self.fastq_path, gensam_fastq_output_path)
        return gensam_fastq_output_path


class RunContext:
    "TODO"
    def __init__(self, input_path):
        self.input_path = input_path.rstrip('/')
        self.input_name = os.path.basename(self.input_path)

        # From data dump of plugin
        self.metadata_name = 'metadata.json'
        self.metadata_path = os.path.join(self.input_path, self.metadata_name)
        self.pangolin_path = os.path.join(self.input_path, 'pangolin.csv')

        self.date = datetime.now().strftime('%Y-%m-%d')
        self.gensam_pangolin_name = f'{REGION_CODE}_{LAB_CODE}_{self.date}_pangolin_classification.csv'
        self.gensam_complementary_name = f'{REGION_CODE}_{LAB_CODE}_{self.date}_komplettering.csv'

    def has_finished_transfer(self):
        """Return bool of completion signal file for transfer completion existing."""
        return os.path.exists(self.metadata_path)

    def read_metadata(self):
        with open(self.metadata_path, 'r') as inp:
            return json.load(inp)

    def create_complementary(self, output_directory_path):
        metadata = self.read_metadata()

        gensam_complementary_output_path = os.path.join(output_directory_path, self.gensam_complementary_name)
        with open(gensam_complementary_output_path, 'w') as out:
            headers = ['provnummer', 'urvalskriterium', 'GISAID_accession']
            print(','.join(headers), file=out)

            for sample_name, sample_metadata in metadata.items():
                selection_criteria_index = int(sample_metadata.split('_')[-1])
                selection_criteria_text = selection_criteria[selection_criteria_index]
                print(','.join([sample_name, selection_criteria_text, '']), file=out)

        return gensam_complementary_output_path

    def reformat_pangolin_csv(self, output_directory_path):
        """
        Reformat the given pangolin csv from IonTorrent plugin format to Fohm format.

        Since we can not send multiple pangolin results, we have to concat with previous.
        """
        gensam_pangolin_output_path = os.path.join(output_directory_path, self.gensam_pangolin_name)

        if not os.path.exists(gensam_pangolin_output_path):
            with open(gensam_pangolin_output_path, 'w') as out:
                new_headers = ['taxon', 'lineage', 'probability', 'pangoLEARN_version', 'status', 'note']
                print('\t'.join(new_headers), file=out)

        with open(gensam_pangolin_output_path, 'a') as out:
            with open(self.pangolin_path, 'r') as inp:
                csv_handle = csv.DictReader(inp, delimiter=',')

                original_headers = ['Sample', 'Lineage', 'probability', 'pangoLEARN_version', 'status', 'note']
                for row in csv_handle:
                    new_row = []
                    for header in original_headers:
                        new_row.append(row[header])
                    print('\t'.join(new_row), file=out)

        return gensam_pangolin_output_path


def file_increment(path):
    """
    Add an incremental int before the suffix of the supplied path.

    Will not work for filenames with an int directly prior to the suffix already.
    """
    basename = os.path.basename(path)
    dirname = os.path.dirname(path)

    name, extension = os.path.splitext(basename)

    if re.search('.+_[0-9]+$', name):
        name, increment = name.rsplit('_', 1)
        increment = int(increment)
    else:
        increment = 0

    next_increment = increment + 1

    if extension:
        return os.path.join(dirname, f'{name}_{next_increment}{extension}')  # . in extension
    else:
        return os.path.join(dirname, f'{name}_{next_increment}')

@click.group()
def cli():
    pass


@cli.command()
@click.argument('input_directory_path', type=click.Path(exists=True))
def single_run(input_directory_path):
    run_single_run(input_directory_path)


def run_single_run(input_directory_path):
    """TODO"""
    run_ctx = RunContext(input_directory_path)

    if not run_ctx.has_finished_transfer():
        raise IncompleteTransferError(f'Could not find completion signal {run_ctx.metadata_name} for {run_ctx.input_path}')

    # Connect to GENSAM sftp and chdir into upload destination
    with pysftp.Connection(GENSAM_URL,
                           username=GENSAM_USER,
                           private_key=GENSAM_SSH_KEY,
                           private_key_pass=GENSAM_SSH_KEY_PASSWORD,
                           log=LOG_PATH) as sftp_connection:
        sftp_connection.chdir(GENSAM_URL_SUBDIRECTORY)

        metadata = run_ctx.read_metadata()
        for sample_name in metadata:
            sample_ctx = SampleContext(sample_name, run_ctx.input_path)

            if not sample_ctx.has_all_files():
                logger.warning(f'Could not find all expected files for {sample_ctx.sample_name}')
                continue

            # Check if file has been uploaded
            with open(UPLOADED_SAMPLE_LIST_PATH, 'r') as inp:
                rows = [row.strip() for row in inp.readlines()]
            if sample_ctx.sample_name in rows:
                logger.warning(f'Already uploaded {sample_ctx.sample_name}')
                continue

            gensam_fasta_path = sample_ctx.reformat_fasta(UPLOAD_PATH)
            gensam_vcf_path = sample_ctx.reformat_vcf(UPLOAD_PATH)
            gensam_fastq_path = sample_ctx.reformat_fastq(UPLOAD_PATH)

            # Perform upload of sample specific files
            # sftp_connection.put(gensam_fasta_path, os.path.basename(gensam_fasta_path))  # TODO
            # sftp_connection.put(gensam_vcf_path, os.path.basename(gensam_vcf_path))  # TODO
            # sftp_connection.put(gensam_fastq_path, os.path.basename(gensam_fastq_path))  # TODO

            with open(UPLOADED_SAMPLE_LIST_PATH, 'a') as out:
                logger.info(f'Adding {sample_ctx.sample_name} to previous upload list')
                print(sample_ctx.sample_name, file=out)

        # Check if run specific files have been uploaded
        with open(UPLOADED_RUN_LIST_PATH, 'r') as inp:
            rows = [row.strip() for row in inp.readlines()]
        if run_ctx.input_name in rows:
            logger.warning(f'Already uploaded {run_ctx.input_name}')
            return   # TODO

        # Perform upload of pangolin result
        gensam_pangolin_path = run_ctx.reformat_pangolin_csv(UPLOAD_PATH)

        # Rename complementary files before email
        # Always renames to contain incremental int
        gensam_complementary_path = run_ctx.create_complementary(UPLOAD_PATH)
        gensam_incremental_complementary_path = file_increment(gensam_complementary_path)
        while os.path.exists(gensam_incremental_complementary_path):  # NOTE: Spooky endless
            gensam_incremental_complementary_path = file_increment(gensam_incremental_complementary_path)
        else:
            os.rename(gensam_complementary_path, gensam_incremental_complementary_path)

        # Email complementary file
        # email_fohm(gensam_incremental_complementary_path)  # TODO

        with open(UPLOADED_RUN_LIST_PATH, 'a') as out:
            logger.info(f'Adding {run_ctx.input_name} to previous upload list')
            print(run_ctx.input_name, file=out)


@cli.command()
def wrapper():
    """Wrapper for finding and running single runs."""
    datadump_run_paths = glob.glob(os.path.join(DATADUMP_ROOT, '*'))  # Should be a non-manual directory

    for datadump_run_path in datamp_run_paths:
        run_single_run(datadump_run_path)

        # Email micro department
        datadump_run_name = os.path.basename(datadump_run_path)
        subject = 'Automatic GENSAM transfer completion'
        body = '\n'.join([f'The following run and its associated files have been uploaded to GENSAM:',
                datadump_run_name])
        # email_micro(subject, body)  # TODO


@cli.command()
def pangolin_uploader():
    """
    Specific entry for uploading pangolin results.

    DO NOT RUN THIS! CRON ONLY!

    Fohm only accept one pangolin file per day, hence this function only running towards midnight.
    """

    # Connect to GENSAM sftp and chdir into upload destination
    with pysftp.Connection(GENSAM_URL,
                           username=GENSAM_USER,
                           private_key=GENSAM_SSH_KEY,
                           private_key_pass=GENSAM_SSH_KEY_PASSWORD,
                           log=LOG_PATH) as sftp_connection:
        sftp_connection.chdir(GENSAM_URL_SUBDIRECTORY)

    # Find the pangolin file
    run_ctx = RunContext('')  # NOTE: Only used for the date and pangolin path
    collective_pangolin_result_path = os.path.join(UPLOAD_PATH, run_ctx.gensam_pangolin_name)

    if os.path.exists(collective_pangolin_result_path):
        # Upload to Fohm
        # sftp_connection.put(collective_pangolin_result_path, os.path.basename(collective_pangolin_result_path))  # TODO

        # Email micro department
        subject = 'Automatic GENSAM collective pangolin completion'
        body = '\n'.join(['A collective pangolin result file was found and uploaded.',
                          'This file is a merger of all pangolin results from the day',
                          'This action is attempted before midnight everyday to circumvent FoHM not allowing multiple pangolin results to be uploaded on a single day.',
                          'This mail is only sent out on actual file uploads.'])
        # email_micro(subject, body)  # TODO


if __name__ == '__main__':
    cli()
