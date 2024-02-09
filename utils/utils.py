
import json
import os
import pickle

import pandas as pd
from urllib.parse import urlparse


def get_rows_between_indices(df, indices):

    indices = sorted(indices)

    if indices[0] < 0 or indices[-1] >= len(df):
        raise ValueError("Invalid indices provided")

    rows_between = pd.concat([df.iloc[start + 1: end] for start, end in zip(indices, indices[1:])] +
                             [df.iloc[indices[-1] + 1:]])

    return rows_between


def is_valid_url(s):
    try:
        result = urlparse(s)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

def json_read(f_name):
    with open(f_name) as file:
        return json.load(file)


def json_write(objs, f_name):
    with open(f_name, "w") as file:
        json.dump(objs, file, indent=4)


def json_update(filename, new_data):
    existing_data = json.load(open(filename, 'r')) if os.path.exists(filename) else []
    json.dump(existing_data + new_data, open(filename, 'w'), indent=2)
    return


def pickle_load(f_path):
    with open(f_path, 'rb') as file:
        return pickle.load(file)


    









