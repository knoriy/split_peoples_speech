# Split peoples speech

This is currently a WIP but in a workable state.

## prequisists

Install Montreal Forced Aligner (MFA). For more information about MFA please visit [here.](https://montreal-forced-aligner.readthedocs.io/en/latest/index.html)

``` bash
conda create -n aligner -c conda-forge -y montreal-forced-aligner
conda activate aligner
conda install pandas praat-textgrids

```

### Install dependencies

```Shell
pip install pandas praat-textgrids
```

## Download dataset

You can find the link for the full dataset [here.](https://mlcommons.org/en/peoples-speech/)

``` bash
wget https://storage.googleapis.com/example_download_data/flac_train_manifest.jsonl # Download maifest
wget https://storage.googleapis.com/example_download_data/subset_flac.tar  # download peoples speech subset
```

## Split audio

``` bash
python main.py
```

alternativly you can use the provided notebook

## TODO

- [ ] Save transcript along with audio
- [ ] Please check to avoid processing samples twice
- [ ] Test with other languages
