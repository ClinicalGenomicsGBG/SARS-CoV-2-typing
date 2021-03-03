import os
import re
import sys
import json
import logging
import requests
import subprocess

from datetime import datetime

from ion.plugin import IonPlugin, RunType, RunLevel, PluginCLI


base_url = 'http://torrentserver04.sa.gu.se'
headers = {'Authorization': 'ApiKey ionadmin:3f2cdec72f8b1783dfc1222cb4f4164c79de53a7'}


class ApiSampleMissingError(Exception):
    """Raise on not finding any expected result for sample name in API sample resource."""


class SeqstoreUploadError(Exception):
    """Raise on trying to run plugin without seqstore upload being finished."""


class microbiology_s5plugin(IonPlugin):
    # The version number for this plugin
    version = "0.0.1.23"

    runtypes = [RunType.COMPOSITE]  # Avoids thumbnails
    runlevel = [RunLevel.LAST]   # Plugin runs after all sequencing done
    depends = ['seqstore_upload']

    seqstore_root = '/results/seqstore/'

    def render_seqstore_upload_error(self):
        """TODO Render on seqstore upload not being run yet."""
        with open('progress_block.html', 'w') as html_handle:
                html_handle.write('<html><body>')
                html_handle.write('Error! Can not run plugin without seqstore_upload running first!')
                html_handle.write('</body></hmtl>')

    def render_plugin_success(self, approved):
        """TODO"""
        with open('progress_block.html', 'w') as html_handle:
            html_handle.write('<html><body>')
            html_handle.write('<h2>HTML Table</h2>')
            html_handle.write('<table>')
            html_handle.write('<tr>')
            html_handle.write('<th>Name</th>')
            html_handle.write('<th>Description</th>')
            html_handle.write('</tr>')

            for sample_info in approved:
                html_handle.write('<tr>')
                html_handle.write('<td>{}</td>'.format(sample_info['name']))
                html_handle.write('<td>{}</td>'.format(sample_info['description']))
                html_handle.write('</tr>')

            html_handle.write('</table>')
            html_handle.write('</body></hmtl>')

    def get_run_info(self):
        """Read run related information."""

        # Instrument
        instrument_name = self.startplugin['expmeta']['instrument']

        # Experiment
        experiment_id = self.startplugin['expmeta']['results_name'].split('_')[-1]

        # Run
        raw_run_name = self.startplugin['expmeta']['run_name']
        run_id, user_set_run_name = re.search('{}-([0-9]+)-(.+)'.format(instrument_name), raw_run_name).groups()  # Lots of crap is prepended to run name, skip it
        run_name = user_set_run_name.replace('_', '-')

        # Report
        raw_report_name = self.startplugin['expmeta']['results_name']
        if instrument_name in raw_report_name:  # If default, instrument_name name is included. If reanalysis, it is only according to user naming.
            report_name = re.search('{}-[0-9]+-(.+)'.format(instrument_name), raw_report_name).group(1)
        else:
            report_name = raw_report_name
        report_id = self.startplugin['runinfo']['pk']  # Readily available id, made available to all through self

        # Dates
        raw_datetime = self.startplugin['expmeta']['run_date']
        raw_date, raw_time, _ = re.split('T|Z', raw_datetime)  # <date>T<time>Z
        sequencing_date = raw_date[2:].replace('-', '')  # From 2018-10-11 to 181011

        # Plugin
        plugin_id = self.startplugin['runinfo']['pluginresult']

        # Seqstore location
        run_report_id = '{}-{}'.format(run_id, report_id)  # Used for naming of directories and files
        seqstore_directory_name = '_'.join([run_report_id, sequencing_date, run_name])
        seqstore_directory_path = os.path.join(self.seqstore_root, seqstore_directory_name)

        # User comment
        user_comment = self.startplugin['pluginconfig'].get('user_comment', '')

        run_info = {'instrument_name': instrument_name,
                    'experiment_id': experiment_id,
                    'run_name': run_name,
                    'report_name': report_name,
                    'run_id': run_id,
                    'report_id': report_id,
                    'run_report_id': run_report_id,
                    'sequencing_date': sequencing_date,
                    'upload_plugin_id': plugin_id,
                    'seqstore_directory_name': seqstore_directory_name,
                    'seqstore_directory_path': seqstore_directory_path,
                    'user_comment': user_comment}

        with open('runinfo.json', 'w') as out:
            json.dump(run_info, out, indent=4)

        return run_info

    def launch(self):
        # Fetch current runs experiment ID
        run_info = self.get_run_info()

        # Fetch selected sample names from barcode table
        barcodetable_entries = self.startplugin['pluginconfig']['barcodetable']
        selected_samples = [entry['sample'] for entry in barcodetable_entries if entry['selected']]

        # Verify that the selected samples have metadata
        base_url = 'http://torrentserver04.sa.gu.se'
        sample_url = '/rundb/api/v1/sample'

        headers = {'Authorization': 'ApiKey ionadmin:3f2cdec72f8b1783dfc1222cb4f4164c79de53a7'}

        report_url = '/rundb/api/v1/results/{}/'.format(run_info['report_id'])
        report_response = requests.get(base_url + report_url, headers=headers)
        report_json = report_response.json()

        approved = []

        for sample_name in selected_samples:
            params = {'name': sample_name}
            sample_response = requests.get(base_url + sample_url, headers=headers, params=params)

            if not sample_response:
                raise ApiSampleMissingError('Could not find API sample page for {}'.format(sample_name))

            sample_json = sample_response.json()

            # Loop through hits, could be multiple
            for obj in sample_json['objects']:

                # Check if hit corresponds to current run incase of multiple
                for exp in obj['experiments']:
                    if exp.split('/')[-2] == run_info['experiment_id']:
                        description = obj['description']

                        sample_info = report_json['metaData'].get('sample_info', None)

                        if not sample_info:
                            self.render_seqstore_upload_error()
                            raise SeqstoreUploadError('Could not find expected seqstore_upload api metadata')

                        for info in sample_info:
                            if info['sample_id'] == sample_name:
                                fastq_path = info['fastq_path']
                                break

                        seqstore_fastq_root = '/seqstore/instruments/s5_01_mik/'
                        fastq_directory_name = fastq_path.split('/')[3]  # NOTE Dumb but probably works
                        fastq_name = os.path.basename(fastq_path)
                        seqstore_fastq_path = os.path.join(seqstore_fastq_root,
                                                           fastq_directory_name,
                                                           fastq_name)

                        compiled = {'name': sample_name,
                                    'description': description,
                                    'metadata': {},  # TODO
                                    'fastq_name': fastq_name,
                                    'torrent_fastq_path': fastq_path,
                                    'seqstore_fastq_path': seqstore_fastq_path}

                        approved.append(compiled)

                    else:
                        continue

        compiled_output_path = os.path.join(run_info['seqstore_directory_path'],
                                            'compiled.json')  # TODO Namechange
        with open(compiled_output_path, 'w') as out:
            json.dump(approved, out, indent=4)

        self.render_plugin_success(approved)

        return True

    # Return list of columns you want the plugin table UI to show.
    # Columns will be displayed in the order listed.
    def barcodetable_columns(self):
        return [
            {"field": "selected", "editable": True},
            {"field": "barcode_name", "editable": False},
            {"field": "sample", "editable": False}]


if __name__ == "__main__":
    PluginCLI()
