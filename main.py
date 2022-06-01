import os
import glob
import tqdm
import textgrids
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import flac_to_wav, generate_txt, get_subset_df


# Generate TextGrid alignments
def generate_alignments(src, dest, overwrite=True):
    # if os.path.exists(dest): raise Warning("Desitination Folder already exists")
    os.system(f'mfa align --clean {src} english english {dest}')

def generate_textgrids(dataset_root_path):
    generate_alignments(dataset_root_path, f"{dataset_root_path}_textgrids")

# Split audio into 5-10 seconds
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

def split_audio(path:str, root_wav_path:str):
    textgrid = textgrids.TextGrid(path)
    textgrid_words = textgrid.get('words')

    word_index, split_time = get_longest_silance(textgrid_words)

    # get src path
    wav_paths = os.path.split(path)
    wav_file_name = f'{str(wav_paths[-1]).split(".")[0]}.flac'
    wav_folder_name = os.path.split(wav_paths[0])[-1]

    src_wav_path = os.path.join(root_wav_path, wav_folder_name, wav_file_name) # remove aligned and replace file extension

    # create destinaltion path
    processed_path = os.path.join(f'{root_wav_path}_split', wav_folder_name)
    os.makedirs(processed_path, exist_ok=True)
    dest_path = os.path.join(processed_path, f"{wav_file_name.split('.')[0]}")

    # Split audio
    os.system(f"ffmpeg -loglevel error -i {src_wav_path} -f segment -segment_times {split_time} {dest_path}_%02d.flac")

    # Split text
    sentences = [[word.text for word in textgrid_words[:word_index]], [word.text for word in textgrid_words[word_index:]]]
    for index, sentence in enumerate(sentences):
        with open(f'{dest_path}_{index:02}.txt', 'w') as file:
            file.write(' '.join(sentence))

def convert_all_to_wav(df, base_dataset_path):
    threads= []

    with ThreadPoolExecutor(max_workers=12) as executor:
        for row in tqdm.tqdm(df.iloc, desc="Converting .flac files to .wav"):
            flac_path = os.path.join(base_dataset_path, f'{row["audio_filepath"]}')
            wav_path = os.path.join(base_dataset_path, f'{row["audio_filepath"].split(".")[0]}.wav')
            threads.append(executor.submit(flac_to_wav, flac_path, wav_path, overwrite=True))

def save_all_text_to_file(df):
    threads= []

    with ThreadPoolExecutor(max_workers=12) as executor:
        for row in tqdm.tqdm(df.iloc, desc="Generating .txt files for MFA"):
            threads.append(executor.submit(generate_txt, f'./mini_subset/{row["audio_filepath"].split(".")[0]}.txt', row["text"]))

def split_all_audiofiles(root_textgrid_path, root_wav_path):
    threads= []

    textgrid_paths = glob.glob(f'{root_textgrid_path}/**/*.TextGrid', recursive=True)

    with ThreadPoolExecutor(max_workers=12) as executor:
        for path in tqdm.tqdm(textgrid_paths, desc='spliting flac files into 5-10 seconds'):
            threads.append(executor.submit(split_audio, path, root_wav_path))

if __name__ == '__main__':

    import tarfile
    import shutil
    from utils import generate_txt, get_subset_df

    import warnings
    import fsspec

    filename = '/home/knoriy/Documents/laion/split_peoples_speech/mini_subset.tar.xz'
    root_path = '/home/knoriy/Documents/laion/split_peoples_speech/'
    dataset_name = 'mini_subset'

    chunk = 15


    s3_dataset = fsspec.open('s3://s-laion/peoples_speech/mini_subset.tar.xz')
    s3_dest = fsspec.open('s3://s-laion/peoples_speech/mini_subset.tar.xz')

    with s3_dataset as src_file, s3_dest as dest_file:
        with tarfile.open(fileobj=src_file, mode='r') as src_file_obj, tarfile.open(fileobj=src_file, mode='r') as src_file_obj:
            file_names_full_list = src_file_obj.getnames()
            file_names_full_list = [i for i in file_names_full_list if '.flac' in i]

            for i in range(0, len(file_names_full_list), chunk):
                for file_name in file_names_full_list[i:i + chunk]:
                    src_file_obj.extract(file_name, './')

                generate_subset_tsv = True
                if generate_subset_tsv == True:
                    df = get_subset_df('/home/knoriy/Documents/laion/split_peoples_speech/mini_subset/**/*.flac')
                
                # Save transcript to file
                save_all_text_to_file(df)

                # Convert Flac to wav
                convert_all_to_wav(df, os.path.join(root_path, dataset_name))

                # Get audio text alignments and split audio
                generate_textgrids(os.path.join(root_path, dataset_name))
                split_all_audiofiles('/home/knoriy/Documents/laion/split_peoples_speech/mini_subset_textgrids', os.path.join(root_path, dataset_name))
                
                warnings.warn('Not uploading split file to s3')
                split_files = glob.glob('/home/knoriy/Documents/laion/split_peoples_speech/mini_subset_textgrids/**/*.*')

                for split_file in split_files:
                    src_file_obj.addfile(split_file)

                # y_n = input('continue? y/n: ')
                # if y_n == 'y':
                #     pass

                shutil.rmtree('/home/knoriy/Documents/laion/split_peoples_speech/mini_subset')
                shutil.rmtree('/home/knoriy/Documents/laion/split_peoples_speech/mini_subset_textgrids')
                shutil.rmtree('/home/knoriy/Documents/laion/split_peoples_speech/mini_subset_split')
