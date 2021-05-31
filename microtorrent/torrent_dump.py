import os
import re
import csv
import json
import glob
import traceback
import shutil

from Bio import SeqIO
from Bio.SeqRecord import SeqRecord
from collections import defaultdict, Counter
from pprint import pprint as pp


def map_sample_id_barcode(json_path):
    """Read the run ion_params_00.json to find mapping between IonCode and Lab set name."""
    with open(json_path, 'r') as inp:
        json_data = json.load(inp)

    mapping = {}
    for sample_name, info in json_data['experimentAnalysisSettings']['barcodedSamples'].items():
        barcode = info['barcodes'][0]  # NOTE: Assumes no multiple barcodes per sample

        mapping[barcode] = sample_name

    return mapping


def find_split_barcode_samples(barcode_id_mapping):
    """Return sample ids found on multiple barcodes."""
    counter = Counter(barcode_id_mapping.values())
    multiple = [sample_id for sample_id, number in counter.items() if number > 1]
    return multiple


def parse_pangolin_result(pangolin_result_path):
    """Parse csv into more fun to read format."""
    collected_info = {}
    with open(pangolin_result_path, 'r') as inp:
        csv_handle = csv.DictReader(inp)

        for row in csv_handle:
            barcode = row.pop('Barcode')
            collected_info[barcode] = row

    return collected_info


def get_sample_fasta(barcode, fasta_path):
    for seq_record in SeqIO.parse(fasta_path, "fasta"):
        if seq_record.id.startswith(barcode):
            return seq_record
            # return seq_record.id, seq_record.seq
    else:
        raise Exception(f'Found no fasta sequence for {barcode}')

def get_sample_vcf(barcode, variant_call_run):
    return os.path.join(variant_call_run['plugin_path'], barcode, f'TSVC_variants_{barcode}.vcf.gz')



class PluginResult:
    def __init__(self, plugin_path):
        # self._sample_regex = 'IonCode_[0-9]+$'
        self.plugin_path = plugin_path
        self.startplugin_path = os.path.join(plugin_path, 'startplugin.json')
        self.barcode_mapping = map_sample_id_barcode(self.startplugin_path)
        # self.sample_output_paths = [os.path.join(self.plugin_path, barcode) for barcode in self.barcode_mapping.keys()]




class VariantCaller(PluginResult):
    def __init__(self, plugin_path):
        super().__init__(plugin_path)

    def get_vcfs(self):
        barcode_vcf_paths = {}
        for barcode, sample_id in self.barcode_mapping.items():
            vcf_path = os.path.join(self.plugin_path, barcode, f'TSVC_variants_{barcode}.vcf.gz')
            barcode_vcf_paths[barcode] = vcf_path

        return barcode_vcf_paths


class TorrentRun:
    def __init__(self, run_path):
        self.run_path = run_path
        self.run_name = os.path.basename(self.run_path)
        self.plugin_root_path = os.path.join(run_path, 'plugin_out')
        self.raw_plugin_paths = glob.glob(os.path.join(self.plugin_root_path, '*'))
        self.plugin_result_paths = {}
        self.ion_params_json_path = os.path.join(run_path, 'ion_params_00.json')



    def get_plugin_paths(self, plugin_name):  # TODO RENAME
        plugin_dicts = []

        expected_plugin_paths = glob.glob(os.path.join(self.plugin_root_path, f'{plugin_name}_out.*'))
        if not expected_plugin_paths:
            raise Exception(f'Found no plugin paths for given name: {plugin_name}')

        for plugin_path in expected_plugin_paths:
            i = os.path.basename(plugin_path).split('_out.')[-1]

            plugin_dicts.append({'plugin_id': int(i),
                                 'plugin_path': plugin_path})
        return plugin_dicts





def main():
    root_path = '/results/analysis/output/Home/'
    regex = 'run[0-9]{2}'

    all_run_paths = glob.glob(os.path.join(root_path, '*'))
    all_covid_runs = [run for run in all_run_paths if re.search(regex, run, re.IGNORECASE)]

    # Filter away thumbnail, tn
    all_covid_runs = [run for run in all_covid_runs if '_tn' not in run]


    good_run_paths = defaultdict(list)
    for run_path in all_covid_runs:
        regex = 'run([0-9]{2})'

        match = re.search(regex, run_path, re.IGNORECASE)

        try:
            number = match.groups()[0]
            if int(number) < 15:
                continue
            good_run_paths[number].append(run_path)
        except AttributeError:
            print(f'error {run_path}')
            continue


    for number, run_paths in good_run_paths.items():
        for run_path in run_paths:
            try:
                TR = TorrentRun(run_path)
                barcode_id_mapping = map_sample_id_barcode(TR.ion_params_json_path)

                # NOTE: If this happens, handle it then, otherwise ignore
                multiple = find_split_barcode_samples(barcode_id_mapping)
                if multiple:
                    raise Exception(f'Found samples with multiple barcodes in run {TR.run_name}: {multiple}')  # NOTE No support for this hence exception

                try:
                    pangolin_runs = TR.get_plugin_paths('SARS_CoV2_Pangolin')
                    latest_pangolin_run = max(pangolin_runs, key=lambda x: x['plugin_id'])

                    variantcall_runs = TR.get_plugin_paths('variantCaller')
                    latest_variantcall_run = max(variantcall_runs, key=lambda x: x['plugin_id'])

                    pangolin_consensus_fasta_path = glob.glob(os.path.join(latest_pangolin_run['plugin_path'], 'R*.fasta'))[0]
                    pangolin_result_path = glob.glob(os.path.join(latest_pangolin_run['plugin_path'], 'R*.xls'))[0]  #NOTE: It's not xls, its really .csv
                except Exception:  # NOTE: Probably not completed plugin running
                    continue

                pangolin_result = parse_pangolin_result(pangolin_result_path)

                for barcode, sample_id in barcode_id_mapping.items():
                    # sample_id = sample_id.replace(' ', '')  # No one likes whitespaces in names
                    try:
                        if pangolin_result[barcode]['status'] == 'passed_qc' and pangolin_result[barcode]['passes'] == 'Passed':
                            # fasta_name, fasta_sequence = get_sample_fasta(barcode, pangolin_consensus_fasta_path)
                            fasta_record = get_sample_fasta(barcode, pangolin_consensus_fasta_path)
                            variant_call_vcf = get_sample_vcf(barcode, latest_variantcall_run)

                            # Transfer to output directory
                            output_path = '/results/seqstore/covid_datadump/'  # FIXME

                            # Transfer fasta
                            fasta_output_path = os.path.join(output_path, f'14_SE300_{sample_id}.consensus.fasta')
                            if not os.path.exists(fasta_output_path):
                                with open(fasta_output_path, 'w') as out:
                                    SeqIO.write(fasta_record, out, "fasta")

                            # Transfer vcf
                            vcf_output_path = os.path.join(output_path, f'14_SE300_{sample_id}.vcf')
                            if not os.path.exists(vcf_output_path):
                                shutil.copyfile(variant_call_vcf, vcf_output_path)

                        else:
                            continue
                    except (IndexError, KeyError):
                        continue

            except Exception as e:
                print('Following run produced an exception:')
                print(run_path)
                print('This could be due to a run not yet completing its plugin runs. Please verify manually before troubleshooting.')
                formatted_exception = traceback.format_exc()
                print(formatted_exception)


if __name__ == '__main__':
    main()
