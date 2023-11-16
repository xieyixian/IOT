# -*- coding: utf-8 -*-
import os
import json
import pandas as pd
from bs4 import BeautifulSoup
import requests

def get_data(url):
    f = requests.get(url)
    soup = BeautifulSoup(f.content, "lxml")
    str_json = str(soup.body).strip('</p></body>')
    data_json = json.loads(str_json)

    data_new = pd.DataFrame()
    for i in range(len(data_json)):
        data_new.loc[i, 'Value'] = data_json[i].get('Value', '')
        data_new.loc[i, 'Timestamp'] = data_json[i].get('Timestamp', '')
        print('we are done : ', i)
    print('result: \n', data_new)
    return data_new

if __name__ == '__main__':
    url = "http://uoweb3.ncl.ac.uk/api/v1.1/sensors/PER_NE_CAJT_NCA189_SJB1_SJB2" \
          "/data/json/?starttime=20230117&endtime=20230118"
    get_data(url)
