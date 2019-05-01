import os
import pandas as pd
from datetime import datetime
import glob
from pandas.core.indexing import IndexingError


def get_path(subfolder=None):
    if subfolder:
        return os.getcwd() + '/' + subfolder + '/'
    return os.getcwd() + '/'


# TODO create common config with HOME_DIR param and METADATA_DIR
def create_folder_if_not_exists(dirname='metadata'):
    path = get_path(dirname)
    if os.path.exists(path) is not True:
        os.mkdir(path)
    return path


def save_file(payload: pd.DataFrame, path):
    fname = datetime.today().strftime('%d-%m-%Y') + '.csv'
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


def normalize_data(df):
    res = df.copy()
    res = res / res.ix[0, :]
    return res


def compute_daily_returns(df):
    """Compute and return the daily return values."""
    res = df.copy()
    res[1:] = (df[1:] / df[:-1].values) - 1
    try:
        res.ix[0, :] = 0  # replace .ix
    except IndexingError:
        res.ix[0] = 0
    return res
