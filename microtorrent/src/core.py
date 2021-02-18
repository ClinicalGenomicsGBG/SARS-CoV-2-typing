import os
import re
import json
from glob import glob

from definitions import CONFIG_PATH, PREVIOUS_PATH


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


def test_run():
    """TODO"""

    print('running')

    with open(CONFIG_PATH, 'r') as conf:
        config = json.load(conf)

    mount_path = config['seqstore']['mount_path']
    run_regex = config['seqstore']['run_regex']

    available_runs = find_runs(mount_path, run_regex)

    previous_runs = read_previous(PREVIOUS_PATH)

    for run_path in available_runs:
        run_name = os.path.basename(run_path)

        if run_name in previous_runs:
            continue

        info = {run_name: ""}
        write_previous(PREVIOUS_PATH, info)
