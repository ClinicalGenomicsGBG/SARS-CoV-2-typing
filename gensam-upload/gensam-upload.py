#!/usr/bin/env python

@click.command()
@click.option('-r', '--runid', required=True,
              help='Path to Eurofins data download directory')
@click.option('-l', '--logfile', required=True,
              default='/medstore/logs/pipeline_logfiles/sars-cov-2-typing/GENSAM-upload.log',
              help='Path to log file')
def main(runid, logfile):
    #Find all fastq files and make links with correct names
    
    

if __name__ == '__main__':
    main()
