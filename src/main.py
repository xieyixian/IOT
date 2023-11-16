import requests
import pandas as pd

from src.consumer_emqx import consumer_data_emqx
from src.getRequests import get_data
from src.product_emqx import send_data_emqx

if __name__ == '__main__':

    # Target URL
    url = "http://uoweb3.ncl.ac.uk/api/v1.1/sensors/PER_NE_CAJT_NCA189_SJB1_SJB2" \
          "/data/json/?starttime=20230601&endtime=20230603"

    #  data = pd.DataFrame()
    data = get_data(url)
    # print(data.loc[286] )
    # for i in range(len(data)):
    #     send_data_emqx(data.loc[i])

    send_data_emqx(data)




