import os
import tqdm
import ast
import pandas as pd
import glob


def get_subset_df(json_dir=None):
    if not json_dir:
        json_dir = '/home/knoriy/Documents/laion/split_peoples_speech/flac_train_manifest.jsonl'

    with open(json_dir, 'r') as json_file:
        json_list = list(json_file)

    df = pd.DataFrame.from_dict(json_list)
    df[0] = df[0].apply(lambda x: ast.literal_eval(x))

    for keys in df[0][0].keys():
        df[keys] = [path[0][keys] for path in df.iloc()]

    subset = glob.glob('/home/knoriy/Documents/laion/split_peoples_speech/subset/**/*.flac', recursive=True)
    subset = [os.path.join(*(dir.split(os.path.sep)[7:])) for dir in subset]

    subset_df = df[df['audio_filepath'].isin(subset)].reset_index(drop=True)
    subset_df = subset_df.drop(0, axis=1)

    return subset_df

def flac_to_wav(audio_in_path, audio_out_path, no_log=True, overwrite=False):
    log_cmd = ' -v quiet' if no_log else ''
    overwrite_cmd = ''
    if overwrite:
        overwrite_cmd = '-y'

    os.system(f'ffmpeg {overwrite_cmd} -i {audio_in_path} {log_cmd} {audio_out_path}')

def generate_txt(df):
    for row in tqdm.tqdm(df.iloc, desc="Generating .txt files for MFA"):
        open(f'./subset/{row["audio_filepath"].split(".")[0]}.txt', 'w').write(row['text'])