import os
import tqdm

def flac_to_wav(audio_in_path, audio_out_path, no_log=True, overwrite=False):
    log_cmd = ' -v quiet' if no_log else ''
    overwrite_cmd = ''
    if overwrite:
        overwrite_cmd = '-y'

    os.system(f'ffmpeg {overwrite_cmd} -i {audio_in_path} {log_cmd} {audio_out_path}')

def generate_txt(df):
    for row in tqdm.tqdm(df.iloc, desc="Generating .txt files for MFA"):
        open(f'./subset/{row["audio_filepath"].split(".")[0]}.txt', 'w').write(row['text'])

