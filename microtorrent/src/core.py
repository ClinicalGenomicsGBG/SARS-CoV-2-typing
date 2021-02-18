import os
import re
import json
from glob import glob

from definitions import CONFIG_PATH, PREVIOUS_PATH


class SampleFastq:
    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = os.path.basename(self.filepath)

        self._split(filepath)

    def _split(self, path):
        basename = os.path.basename(path)
        barcode, name, runreport_id, date = basename.split('_')
        run_id, report_id = runreport_id.split('-')

        self.barcode = barcode
        self.name = name
        self.run_id = run_id
        self.report_id = report_id
        self.date = date


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
        run_name = os.path.basename(run_path)

        # Skip already parsed runs
        # TODO Change to sample basis
        if run_name in previous_runs:
            continue

        info = {run_name: ""}
        write_previous(PREVIOUS_PATH, info)

        sample_regex = config['seqstore']['sample_regex']
        sample_paths = find_samples(run_path, sample_regex)

        for sample_path in sample_paths:
            Sample = SampleFastq(sample_path)
