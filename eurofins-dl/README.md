# Eurofins data downloader

This contains two scripts:

1. sync-sftp.sh - syncs the eurofins sFTP with a folder on medstore
2. microReport.py - copies over the pangolin file to a outbox so microbiology can download from there

The idea is to run these scripts in a wrapper in a cronjob. Perhaps once per day or so. Or just after every time Eurofins deliver new data.
