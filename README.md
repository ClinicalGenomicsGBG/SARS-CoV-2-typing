# sars-cov-2-typing

## Usage

### View help page

```python
./hcp_covid.py -h
```

```python
usage: hci_covid.py [-h] [-ep ENDPOINT] [-aki AWS_ACCESS_KEY_ID]
                    [-sak AWS_SECRET_ACCESS_KEY] [-b BUCKET] [-p PATH]
                    [-f FILEPATH] [-l] [-d] [-q QUERY] [-o OUTPUT] [-k KEY]
                    [-e] [-i]

optional arguments:
  -h, --help            show this help message and exit
  -e, --eurofins        check for eurofins files automatically
  -i, --direkttest      check for direkttest files automatically

required arguments:
  -ep ENDPOINT, --endpoint ENDPOINT
                        endpoint url
  -aki AWS_ACCESS_KEY_ID, --aws_access_key_id AWS_ACCESS_KEY_ID
                        aws access key id
  -sak AWS_SECRET_ACCESS_KEY, --aws_secret_access_key AWS_SECRET_ACCESS_KEY
                        aws secret access key
  -b BUCKET, --bucket BUCKET
                        bucket name

additional required arguments for upload or download:
  -p PATH, --path PATH  path to directory with files for upload
  -f FILEPATH, --filepath FILEPATH
                        path to single file
  -l, --listfiles       list existing files
  -d, --download        Download files, -k for single file, -q for files found
                        using query
  -q QUERY, --query QUERY
                        search for files on HCP
  -o OUTPUT, --output OUTPUT
                        outputpath for downloaded file
  -k KEY, --key KEY     filepath on HCP (key) for file to download

```

### Will list all files related to covid-wgs for specified bucket

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b orebro --listfiles
```

### Will list all files containing specified query

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b goteborg -q <query>
```

### Upload one file

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b <bucketname> -f <single-file>
```

### Upload files by giving path to a directory 

If everything in a directory is going to be uploaded just write `*`. The script will upload every file in the directory except md5sum.txt and the original pangolin result file. Only the pangolin file with `*fillempty.txt` in it will be uploaded (the empty cells have been replace with NULL to work with the indexing)

> Files containing "R2"
```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b <bucketname> -p "path/to/files*R2*"
```

### Downloading files
One at a time (specific path using --key)

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b <bucketname> -k <filename on HCP> -o <path/to/outputdir> --download
```

Several files related to a query (--query, e.g. every file containing "CO-O67")

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b <bucketname> -q <query> -o <path/to/outputdir> --download
```
