import os
import re
import json
import requests
from glob import glob

from definitions import CONFIG_PATH, PREVIOUS_PATH


class RunClass:
    def __init__(self, run_path):
        self.run_path = run_path
        self.run_name = os.path.basename(self.run_path)

        self._split(self.run_name)

    def _split(self, run_name):
        """Split runpath into components."""
        runreport_id, date, name = run_name.split('_')
        run_id, report_id = runreport_id.split('-')

        self.name = name
        self.run_id = run_id
        self.report_id = report_id
        self.date = date


class SampleClass:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_name = os.path.basename(self.file_path)

        self._split(self.file_name)

    def _split(self, file_name):
        """Split file_path into components."""
        basename = os.path.basename(file_name)
        barcode, name, runreport_id, date = basename.split('_')
        run_id, report_id = runreport_id.split('-')

        self.barcode = barcode
        self.name = name
        self.run_id = run_id
        self.report_id = report_id
        self.date = date

# TORRENT API

def query_api(url):
    # TODO CONFIG
    user = 'ionadmin'
    api_key = '3f2cdec72f8b1783dfc1222cb4f4164c79de53a7'
    headers = {'Authorization': 'ApiKey {}:{}'.format(user, api_key)}
    return requests.get(url, headers=headers)

# ---------------------


# LOCAL SNIFF
def find_runs(root_path, regex):
    """Return run paths in root_path matching given regex on basename."""
    return [path for path in glob(f'{root_path}/*') if re.search(regex, path)]


def read_previous(path):
    """Return info contained in previous runs json."""
    with open(path, 'r') as inp:
        return json.load(inp)


def write_previous(path, info):
    """Update info contained in previous runs json."""
    previous_runs = read_previous(path)

    keys = info.keys()

    for key in keys:
        if key in previous_runs:
            raise Exception(f'Attempted overwrite of existing information: {key}')

    previous_runs.update(info)

    with open(path, 'w') as out:
        json.dump(previous_runs, out, indent=4)

    return path


def find_metadata(root_path):
    """Assumes metadata placed locally by bioinformatics."""
# --------------------


def find_samples(root_path, regex):
    sample_paths = glob(f'{root_path}/*')
    return [path for path in sample_paths if re.search(regex, os.path.basename(path))]


def test_run():
    """TODO"""
    with open(CONFIG_PATH, 'r') as conf:
        config = json.load(conf)

    mount_path = config['seqstore']['mount_path']
    run_regex = config['seqstore']['run_regex']

    # Parse for all runs
    available_runs = find_runs(mount_path, run_regex)

    # Load previous runs
    previous_runs = read_previous(PREVIOUS_PATH)

    for run_path in available_runs:
        Run = RunClass(run_path)

        # Skip already parsed runs
        # TODO Change to sample basis
        if Run.name in previous_runs:
            continue

        # Check if run belongs to project of relevance
        instrument_url = "http://torrentserver04.sa.gu.se"
        results_suburl = f"rundb/api/v1/results/{Run.report_id}"
        response = query_api(os.path.join(instrument_url, results_suburl))

        if not response:
            continue

        resp_json = response.json()

        projects_suburls = resp_json['projects']
        for projects_suburl in projects_suburls:
            try:
                project_pk = projects_suburl.split('/')[-2]  # -2 due to trailing /
            except IndexError:  # If no projects
                continue

            if project_pk == '26':
                break
        else:
            continue  # If no matching project

        info = {Run.name: ""}
        write_previous(PREVIOUS_PATH, info)

        sample_regex = config['seqstore']['sample_regex']
        sample_paths = find_samples(run_path, sample_regex)

        for sample_path in sample_paths:
            Sample = SampleClass(sample_path)
