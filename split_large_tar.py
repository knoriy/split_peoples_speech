import tarfile
import glob
import os
import tqdm
import shutil 

import os

def make_tarfile(source_dir, output_filename):
    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))
    with tarfile.open(output_filename, "w") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

    return output_filename

def split_large_tar(src_tar:str, dest_path:str, dataset_name:str, chunk_size:int, s3_dest):
    with tarfile.open(src_tar, mode='r') as src_file_obj:
        print("opened file")

        file_names_full_list = src_file_obj.getnames()
        print("got names")

        for i in tqdm.tqdm(range(0, len(file_names_full_list), chunk_size), desc='Chunks remaining: '):
            for file_name in tqdm.tqdm(file_names_full_list[i:i + chunk_size], desc="Extracting Files: "):
                src_file_obj.extract(file_name, f'./tmp/')
            
            tar_file_path = make_tarfile( './tmp/', os.path.join(dest_path, f'{i}.tar'))
            s3_dest.put(tar_file_path, f's-laion/peoples_speech/{dataset_name}_split_tars/{os.path.basename(tar_file_path)}')
            shutil.rmtree('./tmp/')
            shutil.rmtree(os.path.dirname(tar_file_path))


if __name__ == '__main__':
    import fsspec

    src_file = '/mnt/knoriy/pps_train.tar'
    dest_file = '/home/knoriy/split_peoples_speech/test_split/'
    dataset_name = 'pps_train'

    s3_dest = fsspec.filesystem('s3')

    split_large_tar(
        src_file, 
        dest_file,
        dataset_name,
        1000,
        s3_dest)