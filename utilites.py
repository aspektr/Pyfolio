import os
import pandas as pd
from datetime import datetime


def create_folder_if_not_exists(path: str, fname='metadata'):
    path = path + '/' + fname
    if os.path.exists(path) is not True:
        os.mkdir(path)
    return path


def save_file(payload: pd.DataFrame, path):
    fname = '/' + datetime.today().strftime('%d-%m-%Y') + '.csv'
    payload.to_csv(path + fname, sep=';')

