import os
import pandas as pd
from datetime import datetime
import glob


# TODO create common config with HOME_DIR param and METADATA_DIR
def create_folder_if_not_exists(path: str, fname='metadata'):
    path = path + '/' + fname
    if os.path.exists(path) is not True:
        os.mkdir(path)
    return path


def save_file(payload: pd.DataFrame, path):
    fname = '/' + datetime.today().strftime('%d-%m-%Y') + '.csv'
    payload.to_csv(path + fname, sep=';')


def get_the_newest_fname(path, pattern):
    list_of_files = glob.glob(path + pattern)
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file


def rotate_files(path, pattern):
    lastf = get_the_newest_fname(path, pattern)
    for f in glob.glob(path + pattern):
        if f != lastf:
            os.remove(f)

