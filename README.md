# sars-cov-2-typing

## Usage

### Will list all files related to covid-wgs for specified bucket

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b orebro --listfiles
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
