#!/usr/bin/env python3

import os
import subprocess
import argparse
import glob

def arg():
    parser = argparse.ArgumentParser(prog="clc_sync.py")
    parser.add_argument("-r", "--run", help="Name of run")
    parser.add_argument("-p", "--password", help="CLC password")
    args = parser.parse_args()
    return args


def clc(password,run,server,port,user):
    log_file=open('/medstore/logs/pipeline_logfiles/sars-cov-2-typing/nextseq_clcimport.log','a')
    # Check if directory exists
    if not os.path.exists(f"/medstore/CLC_Data_Folders/Microbiology/SARS-CoV-2_Clinical/Illumina/{run}"):
        # Create directory on CLC
        cmd = ["/apps/clcservercmdline/clcserver", "-S", server, 
                                                "-P", str(port), 
                                                "-U", user, 
                                                "-W", password, 
                                                "-A", "mkdir", 
                                                "-t", "clc://server/CLC_Data_Folders/Microbiology/SARS-CoV-2_Clinical/Illumina/",
                                                "-n", run]    
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=log_file, shell=False)

        while process.wait() is None:
            pass
        process.stdout.close()

    # Import fasta files to CLC
    for path in glob.glob(f"/medstore/results/clinical/SARS-CoV-2-typing/nextseq_data/{run}/fasta/*.fa", recursive=True):
        # Check if file exists on CLC
        clc_file = "Consensus_"+os.path.basename(path.replace(".fa","*"))
        if not glob.glob(f"/medstore/CLC_Data_Folders/Microbiology/SARS-CoV-2_Clinical/Illumina/{run}/{clc_file}", recursive=True):
            cmd2 = ["/apps/clcservercmdline/clcserver", "-S", server, 
                    "-P", str(port), 
                    "-U", user, 
                    "-W", password, 
                    "-G", "clinical-production", 
                    "-A", "import", 
                    "-f", "fasta", 
                    "-s", f"clc://serverfile/{path}", 
                    "-d", f"clc://server/CLC_Data_Folders/Microbiology/SARS-CoV-2_Clinical/Illumina/{run}"]
            process2 = subprocess.Popen(cmd2, stdout=subprocess.PIPE, stderr=log_file, shell=False)

            while process2.wait() is None:
                pass
            process2.stdout.close()

    log_file.close()

def main():
    args = arg()
    password = args.password
    run = args.run 

    # Variables for CLC upload
    server = "medair.sahlgrenska.gu.se"
    port = 7777
    user = "cmduser"

    clc(password,run,server,port,user)


if __name__ == "__main__":
    main()
