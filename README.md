# Split peoples speech

This is currently a WIP but in a workable state.

## prequisists

Install Montreal Forced Aligner (MFA). For more information about MFA please visit [here.](https://montreal-forced-aligner.readthedocs.io/en/latest/index.html)

``` bash
conda create -n aligner -c conda-forge -y montreal-forced-aligner
conda activate aligner
conda install pandas praat-textgrids

```

### Download MFA Accustic models

``` shell
mfa models download acoustic english_mfa
mfa models download dictionary english_mfa
```

### Install dependencies

```Shell
pip install pandas praat-textgrids
```

## Download dataset

You can find the link for the full dataset [here.](https://mlcommons.org/en/peoples-speech/)

``` bash
nohup sh -c "wget <URL> -O - | aws s3 cp - s3://<S#_bucket>" &

wget https://storage.googleapis.com/example_download_data/flac_train_manifest.jsonl # Download maifest
wget https://storage.googleapis.com/example_download_data/subset_flac.tar  # download peoples speech subset
```

## Split audio

``` bash
python main.py
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

- [X] Save transcript along with audio
- [X] Multithread all passes
- [ ] Split large tar and json into smaller files
- [ ] checks to avoid processing samples twice
- [ ] checks to avoid processing samples twice
- [ ] Test with other languages
