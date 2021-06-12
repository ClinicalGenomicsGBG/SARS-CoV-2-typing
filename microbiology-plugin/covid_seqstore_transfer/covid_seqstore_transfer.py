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
                continue
