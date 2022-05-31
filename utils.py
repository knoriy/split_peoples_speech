import os
import tqdm
import ast
import pandas as pd
import glob


def get_subset_df(dataset_root_path, json_dir=None):
    if not json_dir:
        json_dir = '/home/knoriy/Documents/laion/split_peoples_speech/flac_train_manifest.jsonl'

    with open(json_dir, 'r') as json_file:
        json_list = list(json_file)

    df = pd.DataFrame.from_dict(json_list)
    df[0] = df[0].apply(lambda x: ast.literal_eval(x))

    for keys in df[0][0].keys():
        df[keys] = [path[0][keys] for path in df.iloc()]

    subset = glob.glob(dataset_root_path, recursive=True)
    subset = [os.path.join(*(dir.split(os.path.sep)[7:])) for dir in subset]

    subset_df = df[df['audio_filepath'].isin(subset)].reset_index(drop=True)
    subset_df = subset_df.drop(0, axis=1)

    return subset_df
    # get_subset_df().to_csv('/home/knoriy/Documents/laion/split_peoples_speech/subset.tsv', sep='\t', header=None, index=False)


def flac_to_wav(audio_in_path, audio_out_path, overwrite=False):
    overwrite_cmd = ''
    if overwrite:
        overwrite_cmd = '-y'

    return os.system(f'ffmpeg {overwrite_cmd} -loglevel error -i {audio_in_path} {audio_out_path}')

def generate_txt(dir:str, text:str):
    with open(dir, 'w') as file:
        file.write(text)
