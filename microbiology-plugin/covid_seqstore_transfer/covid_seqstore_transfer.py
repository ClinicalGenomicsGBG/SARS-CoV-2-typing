from ion.plugin import IonPlugin, RunType, RunLevel


def clean_samplename(sample_name):
    """Return clean and formatted samplename in the event of typos."""
    for character in [' ']:
        sample_name.replace(character, '')

    for character in ['_']:
        sample_name.replace(character, '-')

    return sample_name


class covid_seqstore_transfer(IonPlugin):
    version = "0.0.1.0"
    runtypes = [RunType.COMPOSITE]
    runlevel = [RunLevel.LAST]

    def launch(self):
        for sample_name, barcode_info in self.startplugin['plan']['barcodedSamples'].items():
            sample_name = clean_samplename(sample_name)
