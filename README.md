# sars-cov-2-typing

## Usage

> will list all files related to covid-wgs for specified bucket

```python
./hcp_covid.py -ep https://vgtn0008.hcp1.vgregion.se:443 -aki b3JlYnJv -sak  -b orebro --listfiles
```

> upload one file

```python
./hcp_covid.py -ep <endpoint-url> -aki <aws_access_key_id> -sak <aws_secret_access_key> -b <bucketname> -f <single-file>
```

> upload files by giving path to directory


> downloading files
