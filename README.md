# telecom
This repo host my research assistant work at NTU


## main data description
- messy code warning!!
To have general idea about the tables in telecom database, check out the folder noeebooks/.


## dvc
To download the data for each month(only 201308 is available)
```sh
dvc pull -r origin
```


## data processing
- configuration file: cofig/main.yaml
- script: script/preprocess.py

For example, download one of the meta data for tower call area code:
1. in configuration file, edit the *strategy* to *latlon2addr*
2. use the script
```sh
# utilize the docker container
docker compose run --rm preprocess

# python script
python script/preprocess.py
```
