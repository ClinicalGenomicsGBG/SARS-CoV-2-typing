from ion.plugin import IonPlugin, RunType, RunLevel


class covid_seqstore_transfer(IonPlugin):
    version = "0.0.1.0"
    runtypes = [RunType.COMPOSITE]
    runlevel = [RunLevel.LAST]

    def launch(self):
        pass
