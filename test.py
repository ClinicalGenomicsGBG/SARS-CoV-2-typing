import requests
import json
import os
import re

from microtorrent.definitions import CONFIG_PATH, PREVIOUS_PATH

from NGPinterface.hcp import HCPManager


class Previous:
    def __init__(self, path):
        self.path = path

        self.data = self.read_json()

    def read_json(self):
        """Read existing json data."""
        with open(self.path, 'r') as inp:
            return json.load(inp)

    def write_json(self, data):
        """Overwrite existing json data."""
        with open(self.path, 'w') as out:
            json.dump(data, out, indent=4)
        self.data = data

    def update_json(self, data):
        self.data.update(data)

    @property
    def state(self):
        return self.data['state']

    def set_state(self, state):
        allowed = ['error', 'running']

        if state not in allowed:
            raise Exception(f'Disallowed state: {state}')

    def list_runs(self):
        return sorted(self.data['runs'].keys())

    def list_samples(self):
        sample_list = []
        for run, samples in self.data['runs'].items():
            for sample in samples:
                sample_list.append(sample)
        return sample_list


Prev = Previous(PREVIOUS_PATH)

if Prev.state == 'error':
    raise Exception('Package in state "error" due to a previous crash. Set to "running" once fixed.')


# parameters = {'metaData': {}}  # For filtering

with open(CONFIG_PATH, 'r') as conf:
    config = json.load(conf)

user = config['torrent']['credentials']['user']
api_key = config['torrent']['credentials']['api_key']
headers = {'Authorization': 'ApiKey {}:{}'.format(user, api_key)}

base_url = "http://torrentserver04.sa.gu.se"
next_url = '/rundb/api/v1/results/'
parameters = {'limit': 500}

# Parse all results for project belonging
result_urls = []

while next_url:
    print(base_url + next_url)
    response = requests.get(base_url + next_url, headers=headers, params=parameters)

    if not response:
        break

    json_data = response.json()

    for obj in json_data['objects']:
        project_pks = [int(suburl.split('/')[-2]) for suburl in obj['projects']]

        if 26 in project_pks:
            if '_tn' in obj['resultsName']:
                continue

            result_urls.append(obj['resource_uri'])

    next_url = json_data['meta']['next']


hcp_credentials = config['hcp']['credentials']

try:
    hcpm = HCPManager(hcp_credentials['tenant'], hcp_credentials['access_key'], hcp_credentials['secret_access_key'])
    hcpm.attach_bucket('goteborg')
except Exception as e:
    print('HCP connection issues!')  # TODO Log
    # TODO Send mail?
    raise e


sample_names = []

# Get experiment page from result page, then find sample pages
for result_url in result_urls:
    result_json = requests.get(base_url + result_url, headers=headers).json()

    experiment_url = result_json['experiment']

    experiment_json = requests.get(base_url + experiment_url, headers=headers).json()

    for sample_url in experiment_json['samples']:
        sample_result = requests.get(base_url + sample_url, headers=headers).json()

        sample_name = sample_result['name']
        sample_description = sample_result['description']


        sample_regex = config['torrent']['sample_regex']

        if re.search(sample_regex, sample_name):

            if not result_json['metaData'].get('sample_info'):
                print('Not yet uploaded to seqstore.', sample_name)
                continue

            for sample_info in result_json['metaData']['sample_info']:
                if sample_info['sample_id'] == sample_name:
                    fastq_path = sample_info['fastq_path']


                    subpath = fastq_path.strip('/').split('/', 2)


                    print(sample_name, fastq_path)

# NOTE Duplicates 25feb 2021
# DE21-14246
# DE21-15369
# DE21-13726
# DE21-16436
# DE21-16453

{
"date": "2021-02-18T09:35:39.000761+00:00",
"description": "",
"displayedName": "DE21-18461",
"experiments": [
"/rundb/api/v1/experiment/591/"
],
"externalId": "",
"id": 6165,
"name": "DE21-18461",
"resource_uri": "/rundb/api/v1/sample/6165/",
"sampleSets": [],
"status": "run"
}




diccy = {
    'runname': {
        'sample_name': {
            'local_path': '',
            'remote_key': '',
            'date_upload': ''
        }
    }
}
