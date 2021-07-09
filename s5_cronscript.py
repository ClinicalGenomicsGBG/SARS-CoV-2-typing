import os
import json
import shutil
import pysftp
import logging
import logging.handlers

import click

from Bio import SeqIO
from datetime import datetime

from tools.lookup import selection_criteria


LOG_PATH = '/medstore/logs/pipeline_logfiles/sars-cov-2-typing/micro-GENSAM-upload/s5_cronscript'
UPLOAD_PATH = '/medstore/results/clinical/SARS-CoV-2-typing/microbiology_data/iontorrent_gensam'
UPLOADED_LIST_PATH = os.path.join(UPLOAD_PATH, 'previous_uploads.txt')

GENSAM_URL = 'gensam-sftp.folkhalsomyndigheten.se'
GENSAM_USER = 'se300'
GENSAM_SSH_KEY = '/home/cronuser/.ssh/id_rsa'
GENSAM_SSH_KEY_PASSWORD = ''  # NOTE: DO NOT PUSH
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
        self.gensam_vcf_name = f'{self.gensam_basename}.vcf'
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
        return gensam_vcf_output_path

    def reformat_fastq(self, output_directory_path):
        """Copy fastq to correctly named file."""
        gensam_fastq_output_path = os.path.join(output_directory_path, self.gensam_fastq_name)
        shutil.copyfile(self.fastq_path, gensam_fastq_output_path)
        return gensam_fastq_output_path


class RunContext:
    "TODO"
    def __init__(self, input_path):
        self.input_path = input_path
        self.input_name = os.path.basename(self.input_path)

        self.metadata_name = 'metadata.json'
        self.metadata_path = os.path.join(self.input_path, self.metadata_name)

        self.date = datetime.now().strftime('%Y-%m-%d')
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


@click.command()
@click.argument('input_directory_path', type=click.Path(exists=True))
def main(input_directory_path):
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
            with open(UPLOADED_LIST_PATH, 'r') as inp:
                rows = [row.strip() for row in inp.readlines()]
            if sample_ctx.sample_name in rows:
                logger.warning(f'Already uploaded {sample_ctx.sample_name}')
                continue

            gensam_fasta_path = sample_ctx.reformat_fasta(UPLOAD_PATH)
            gensam_vcf_path = sample_ctx.reformat_vcf(UPLOAD_PATH)
            gensam_fastq_path = sample_ctx.reformat_fastq(UPLOAD_PATH)

            # Perform upload of sample specific files
            sftp_connection.put(gensam_fasta_path, os.path.basename(gensam_fasta_path))
            sftp_connection.put(gensam_vcf_path, os.path.basename(gensam_vcf_path))
            sftp_connection.put(gensam_fastq_path, os.path.basename(gensam_fastq_path))

        # Perform upload of run specific files
        gensam_complementary_path = run_ctx.create_complementary(UPLOAD_PATH)
        sftp_connection.put(gensam_complementary_path, os.path.basename(gensam_complementary_path))


if __name__ == '__main__':
    main()
