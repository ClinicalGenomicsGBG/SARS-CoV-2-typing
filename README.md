# sars-cov-2-typing

## Usage

> will list all files related to covid-wgs for specified bucket

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b orebro --listfiles
```

> upload one file

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b <bucketname> -f <single-file>
```

> upload files by giving path to directory (files containing "R2")

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b <bucketname> -p "path/to/files*R2*"
```

> downloading files

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b <bucketname> -d <filename on HCP> -o <path/to/outputdir>
```
