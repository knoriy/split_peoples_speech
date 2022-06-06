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
    os.system(f'mfa align --clean {src} english_mfa english_mfa {dest}')

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

def get_longest_silence(textgrid_words):
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

    word_index, split_time = get_longest_silence(textgrid_words)

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

def convert_all_to_wav(df, base_dataset_path, max_workers=96):
    threads= []
    l = len(df)
    with tqdm.tqdm(total=l, desc="Converting .flac files to .wav") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for row in df.iloc:
                flac_path = os.path.join(base_dataset_path, f'{row["audio_filepath"]}')
                wav_path = os.path.join(base_dataset_path, f'{row["audio_filepath"].split(".")[0]}.wav')
                threads.append(executor.submit(flac_to_wav, flac_path, wav_path, overwrite=True))
            for _ in as_completed(threads):
                pbar.update(1)

def save_all_text_to_file(df, dataset_name, max_workers=96):
    l = len(df)
    with tqdm.tqdm(total=l, desc="Generating .txt files for MFA") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            threads = [executor.submit(generate_txt, f'./{dataset_name}/{row["audio_filepath"].split(".")[0]}.txt', row["text"]) for row in df.iloc]
            for _ in as_completed(threads):
                pbar.update(1)

def split_all_audio_files(root_textgrid_path, root_wav_path, max_workers=96):
    textgrid_paths = glob.glob(f'{root_textgrid_path}/**/*.TextGrid', recursive=True)
    l = len(textgrid_paths)

    with tqdm.tqdm(total=l, desc='spliting flac files into 5-10 seconds') as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            threads = [executor.submit(split_audio, path, root_wav_path) for path in textgrid_paths]
            for _ in as_completed(threads):
                pbar.update(1)


if __name__ == '__main__':

    import tarfile
    import shutil
    import fsspec
    from utils import generate_txt, get_subset_df, genorate_pps_df, make_tarfile

    chunk = 1000
    generate_subset_tsv = True
    pps_df_dir = '/home/knoriy/split_peoples_speech/pps_train.tsv'

    root_path = '/home/knoriy/split_peoples_speech/'
    dataset_name = 'pps_train'

    metadata_dir = "/mnt/knoriy/metadata.json"
    tar_dir = "/mnt/knoriy/pps_train.tar"

    # init Dirs
    dataset_root_path = os.path.join(root_path, f'{dataset_name}')
    dataset_textgrid_path = os.path.join(root_path, f'{dataset_name}_textgrids')
    dataset_split_path = os.path.join(root_path, f'{dataset_name}_split')

    s3 = fsspec.filesystem('s3')
    s3_dest = f's-laion/peoples_speech/{dataset_name}_tars/'


    if os.path.isfile(pps_df_dir):
        pps_df = pd.read_csv(pps_df_dir, sep='\t', header=None, names=['audio_filepath', 'text'])
    else:
        pps_df = genorate_pps_df(metadata_dir)
        pps_df.to_csv(pps_df_dir, sep='\t', header=None, index=False)

    with tarfile.open(tar_dir, mode='r') as src_file_obj:
        print('opening file: This may take some time\n')

        file_names_full_list = src_file_obj.getnames()
        file_names_full_list = [i for i in file_names_full_list if '.flac' in i]

        print('Total Files found', len(file_names_full_list))

        for i in tqdm.tqdm(range(0, len(file_names_full_list), chunk), desc='Chunks remaining: '):
            for file_name in tqdm.tqdm(file_names_full_list[i:i + chunk], desc="Extracting Files: "):
                src_file_obj.extract(file_name, f'./{dataset_name}/')

            if generate_subset_tsv == True:
                df = get_subset_df(f'{dataset_root_path}/**/*.flac', pps_df)

            # Save transcript to file
            save_all_text_to_file(df, dataset_name)

            # Convert Flac to wav
            convert_all_to_wav(df, os.path.join(root_path, dataset_name))

            # Get audio text alignments and split audio
            generate_textgrids(os.path.join(root_path, dataset_name))
            split_all_audio_files(dataset_textgrid_path, dataset_root_path)

            # Upload Split files to s3
            tar_file_path = make_tarfile(f'{dataset_split_path}', f'{dataset_root_path}/{i}.tar')
            s3.put(tar_file_path, os.path.join(s3_dest, os.path.basename(tar_file_path)))
            print('File Uploaded to: ', os.path.join(s3_dest, os.path.basename(tar_file_path)))

            shutil.rmtree(dataset_root_path)
            shutil.rmtree(dataset_textgrid_path)
            shutil.rmtree(dataset_split_path)
