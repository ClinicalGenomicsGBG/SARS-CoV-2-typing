#!/usr/bin/env python3

import json
import pandas as pd
import openpyxl

def metadata_json():
    df = pd.DataFrame(pd.read_excel("/home/xcanfv/Dataexport_direkttest_10rader.xlsx", engine='openpyxl'))
    for index,rows in df.iterrows():
        metafile = open(f"{rows['Referensnummer']}_metadata.json","w")
        metafile.write(json.dumps({"reference": rows["Referensnummer"], 
            "order_date": str(rows[0]), #rows["Beställningsdatum"], 
            "result_date": str(rows["Resultatdatum"]), 
            "result_read": rows[2], 
            "age": rows[3], 
            "gender": rows[4], 
            "region": rows["Region"], 
            "subregion": rows["Subregion"], 
            "postalcode": rows["Postnummer"],  
            "result": rows["Resultat"], 
            "test_type": rows["Testtyp"], 
            "test_typesystem": rows["Testtyp system"], 
            "kitbatch_id": rows["Kitbatch-id"], 
            "lab_reference": rows["Labbreferens"], 
            "lab_id": rows["Labb-id"], 
            "order_code": rows[15], #rows["Beställarkod"], 
            "order_id": rows[16], #rows["Beställar-id"], 
            "results_id": rows["Resultat-id"], 
            "type": rows["Typ"], 
            "fastq": "", 
            "consensus": "", 
            "pipeline_log": "", 
            "pipeline_result": "", 
            "strain": "", 
            "strain_probability": "", 
            "pangolin_path": "", 
            "instrument": ""}, 
            indent=4))
        metafile.close()

metadata_json()


