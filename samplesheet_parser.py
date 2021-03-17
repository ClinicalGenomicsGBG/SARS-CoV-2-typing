#!/usr/bin/env python3

# Parse covid samplesheet run by nextseq
# outputs a json file

from sample_sheet import SampleSheet
import json
 
def sample_sheet():
    Sheet = SampleSheet("/home/xcanfv/GBG_sars-cov-2-typing/sars-cov-2-typing/SampleSheet_CovidSeq_Prep2-3_Pool2_210315.csv")
    data = {}
    for sample in Sheet.samples:
        sample_name = sample['Sample_ID']
        description = sample['Description']

        data[sample['Sample_ID']] = []
        data[sample['Sample_ID']].append({
            'referensnummer': description.split("_")[0],
            'date': description.split("_")[1],
            'runtype': description.split("_")[2],
            'age': description.split("_")[3],
            'gender': description.split("_")[4],
            'lab_reference': description.split("_")[5],
            'postalcode': description.split("_")[6],
            'ct_value': description.split("_")[7]
        })

    with open('data.txt', 'w') as outfile:
        json.dump(data, outfile,indent=4)

def main():
    sample_sheet()


if __name__ == "__main__":
    main()
