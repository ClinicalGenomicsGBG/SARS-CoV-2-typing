
#----- Script for uploading files to HCP server -----#

## usage: "/apps/bio/software/anaconda2/envs/hcp/bin/python3.6 hcp_upload_test.py"

from hcp import HCPManager


hcpm = HCPManager("https://vgtn0004.hcp1.vgregion.se:443", "Y2dndXNlcg==", "189c31f0341784454bd2c323cdf3d548")

hcpm.attach_bucket('ngs-test')

## CHANGE the path_to_files and bucket location ##
hcpm.upload_file('/home/xkocsu/for_test/example.csv', '14april/example.csv')  ## change the path_to_files and bucket location ##



#---------------------------------------------------------------------#
