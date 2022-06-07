# Split peoples speech

This is currently a WIP but in a workable state.

## prequisists

Install Montreal Forced Aligner (MFA). For more information about MFA please visit [here.](https://montreal-forced-aligner.readthedocs.io/en/latest/index.html)

``` bash
conda create -n aligner -c conda-forge -y montreal-forced-aligner
conda activate aligner
conda install --file requirements.txt

```

### Download MFA Accustic models

``` shell
mfa models download acoustic english_mfa
mfa models download dictionary english_mfa
```

### Install dependencies

```Shell
pip install praat-textgrids
```

## Download dataset

You can find the link for the full dataset [here.](https://mlcommons.org/en/peoples-speech/)

``` bash
nohup sh -c "wget <URL> -O - | aws s3 cp - s3://<s3_bucket>" &

wget https://storage.googleapis.com/example_download_data/flac_train_manifest.jsonl # Download maifest
wget https://storage.googleapis.com/example_download_data/subset_flac.tar  # download peoples speech subset
```

## Split audio

``` bash
nohup python main.py > pps_train.out &
```

alternatively you can use the provided notebook

### Output

``` shell
Dataset_root
│
└───folder1
│   │   file_001.txt
│   │   file_002.txt
|   |   ...
│   
└───folder2
|   │   file_001.txt
│   │   file_002.txt
|   |   ...
```

## TODO

- [x] Save json with `{<filename.tar>: <number of samples (int)>}`
- [X] Save sample with number ids e.g. `0.flac`

