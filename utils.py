import os

def flac_to_wav(audio_in_path, audio_out_path, no_log=True, overwrite=False):
    log_cmd = ' -v quiet' if no_log else ''
    overwrite_cmd = ''
    if overwrite:
        overwrite_cmd = '-y'

    os.system(f'ffmpeg {overwrite_cmd} -i {audio_in_path} {log_cmd} {audio_out_path}')
