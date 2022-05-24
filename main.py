import os
import glob
import tqdm
import textgrids
import pandas as pd

from utils import flac_to_wav, generate_txt


def generate_alignments(src, dest, overwrite=True):
    # if os.path.exists(dest): raise Warning("Desitination Folder already exists")
    os.system(f'mfa align --verbose False --clean {src} english english {dest}')

def generate_textgrids(dataset_root_path):
    generate_alignments(dataset_root_path, f"{dataset_root_path}_textgrids")


def get_potential_splits(textgrid_words):

    potential_split_points = []
    for index, word in enumerate(textgrid_words):
        if word.text != "":
            continue
        if word.xmin > 5 and word.xmax < 10: # find split that are longer than 5 sec and shorter than 10
            potential_split_points.append(index)
            # print(word.xmin, word.xmax)

    return potential_split_points

def get_longest_silance(textgrid_words):
    potential_split_points = get_potential_splits(textgrid_words)

    silance_length = 0
    word_index = None
    time = 0

    for index in potential_split_points:
        silance = textgrid_words[index]
        
        if (silance.xmax - silance.xmin) > silance_length:
            silance_length = (silance.xmax - silance.xmin)
            word_index = index

            time = (silance.xmax + silance.xmin) / 2

    return word_index, time

def split_audio(root_textgrid_path, root_wav_path):
    textgrid_paths = glob.glob(f'{root_textgrid_path}/**/*.TextGrid', recursive=True)

    for path in tqdm.tqdm(textgrid_paths, desc='spliting flac files into 5-10 seconds'):
        textgrid = textgrids.TextGrid(path)
        split_time = get_longest_silance(textgrid.get('words'))[1]

        # get src path
        wav_paths = os.path.split(path)
        wav_file_name = f'{str(wav_paths[-1]).split(".")[0]}.flac'
        wav_folder_name = os.path.split(wav_paths[0])[-1]

        src_wav_path = os.path.join(root_wav_path, wav_folder_name, wav_file_name) # remove aligned and replace file extension

        # create destinaltion path
        processed_path = os.path.join(f'{root_wav_path}_split', wav_folder_name)
        os.makedirs(processed_path, exist_ok=True)
        dest_path = os.path.join(processed_path, f"{wav_file_name}_%03d.flac")

        # Split audio
        os.system(f"ffmpeg -i {src_wav_path} -f segment -segment_times {split_time} {dest_path}")



def main():
    df = pd.read_csv('/home/knoriy/Documents/laion/split_peoples_speech/subset.tsv', names=["audio_filepath","duration", "shard_id", "text"], header=None, sep="\t")
    base_pps_dataset_path = '/home/knoriy/Documents/laion/split_peoples_speech/subset'

    generate_txt(df)

    for row in tqdm.tqdm(df.iloc, desc="Converting .flac files to .wav"):
        flac_path = os.path.join(base_pps_dataset_path, f'{row["audio_filepath"]}')
        wav_path = os.path.join(base_pps_dataset_path, f'{row["audio_filepath"].split(".")[0]}.wav')
        flac_to_wav(flac_path, wav_path, overwrite=True, no_log=False)

    generate_textgrids(base_pps_dataset_path)
    split_audio('/home/knoriy/Documents/laion/split_peoples_speech/subset_textgrids', base_pps_dataset_path)


if __name__ == '__main__':
    main()