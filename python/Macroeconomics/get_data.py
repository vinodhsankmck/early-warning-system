import requests
import json
import io
import pandas as pd
import numpy as np
from datapackage import Package

def get_data(series_list, start_year, end_year):
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": series_list,"startyear":start_year, "endyear":end_year, "registrationkey": '0f746cc9b1d8494c97c4e1cc6a52f6da'})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
    return json.loads(p.text)

def make_chunk(area_list):
    chunk_size = 50
    # using list comprehension 
    result_list = [area_list[i * chunk_size:(i + 1) * chunk_size] for i in range((len(area_list) + chunk_size - 1) // chunk_size )]
    return result_list