import os
import subprocess

from datetime import datetime

from ion.plugin import IonPlugin, RunType, RunLevel


class microbiology_s5plugin(IonPlugin):
    # The version number for this plugin
    version = "0.0.1.0"

    runtypes = [RunType.COMPOSITE]  # Avoids thumbnails
    runlevel = [RunLevel.LAST]   # Plugin runs after all sequencing done

    def launch(self):
        pass

    # Return list of columns you want the plugin table UI to show.
    # Columns will be displayed in the order listed.
    def barcodetable_columns(self):
        return [
            {"field": "selected", "editable": True},
            {"field": "barcode_name", "editable": False},
            {"field": "sample", "editable": False}]
