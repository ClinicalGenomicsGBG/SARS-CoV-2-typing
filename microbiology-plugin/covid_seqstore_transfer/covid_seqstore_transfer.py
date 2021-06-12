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
        for sample_name, barcode_info in self.startplugin['plan']['barcodedSamples'].items():
            sample_name = clean_samplename(sample_name)

            if not is_covid_sample(sample_name):
                not_processed_samples.append(sample_name)
                continue
