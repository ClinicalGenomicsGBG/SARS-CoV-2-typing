import re
import config

from ion.plugin import IonPlugin, RunType, RunLevel


def clean_samplename(sample_name):
    """Return clean and formatted samplename in the event of typos."""
    for character in [' ']:
        sample_name.replace(character, '')

    for character in ['_']:
        sample_name.replace(character, '-')

    return sample_name


def is_covid_sample(sample_name):
    return re.search('D[A-Z]2[0-9]-[0-9]+', sample_name, re.IGNORECASE)


class SampleCollection:
    """
    Class for handling all samples part of a run.

    Barcode-samplename mapping is done with barcode as the main access point.
    In theory, a sample may be spread out over several barcodes.
    Plugin results and output names are generated on a barcode basis.
    """
    def __init__(self):
        self.sample_info = {}

    def add_sample(self, barcode, sample_name):
        self.sample_info[barcode] = sample_name

    def remove_sample(self, barcode):
        return self.sample_info.pop(barcode)

    def __iter__(self):
        return self.sample_info


def find_plugin_outputs(plugin_name, root_path):
    plugin_output_paths = glob.glob(os.path.join(root_path, plugin_name, '*'))

    plugin_outputs = {}
    for plugin_output_path in plugin_output_paths:
        index = plugin_output_path.split('.')[-1]
        plugin_outputs[index] = plugin_output_path

    return plugin_outputs


def parse_pangolin_csv(pangolin_csv_path):
    """Parse csv into more fun to read format."""
    collected_info = {}
    with open(pangolin_csv_path, 'r') as inp:
        csv_handle = csv.DictReader(inp)

        for row in csv_handle:
            barcode = row.pop('Barcode')
            collected_info[barcode] = row

    return collected_info


class covid_seqstore_transfer(IonPlugin):
    version = "0.0.1.0"
    runtypes = [RunType.COMPOSITE]
    runlevel = [RunLevel.LAST]
    depends = [config.pangolin_plugin_name, config.variant_caller_name]

    def launch(self):

        root_report_path = self.startplugin['runinfo']['report_root_dir']

        run_name = self.startplugin['expmeta']['run_name']
        result_name = self.startplugin['expmeta']['results_name']
        # instrument_name = self.startplugin['expmeta']['instrument']
        # experiment_id = self.startplugin['expmeta']['results_name'].split('_')[-1]

        # Divide found samples into covid & non-covid, add barcode information
        covid_samples = SampleCollection()
        non_covid_samples = SampleCollection()
        for sample_name, barcode_info in self.startplugin['plan']['barcodedSamples'].items():
            sample_name = clean_samplename(sample_name)
            sample_barcodes = barcode_info['barcodes']

            if not is_covid_sample(sample_name):
                for barcode in sample_barcodes:
                    non_covid_samples.add_sample(barcode, sample_name)

            for barcode in sample_barcodes:
                covid_samples.add_sample(barcode, sample_name)

        # Parse for plugin outputs
        root_plugin_output_path = os.path.join(root_report_path, 'plugin_out')

        # Pangolin
        plugin_name = config.pangolin_plugin_name
        plugin_outputs = find_plugin_outputs(plugin_name, root_plugin_output_path)
        latest_plugin_output_id = max(plugin_outputs, key=plugin_outputs.get)
        latest_plugin_output_path = plugin_outputs[latest_plugin_output_id]

        pangolin_csv_path = os.path.join(latest_plugin_output_path, '{}.xls'.format(run_name))  #NOTE: It's actually a csv
        pangolin_csv_info = parse_pangolin_csv(pangolin_csv_path)
                continue
