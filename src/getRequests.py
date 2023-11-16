import requests
import pandas as pd
def get_data(url):
    # Request data from Urban Observatory Platform
    resp = requests.get(url)

    # Convert response(Json) to dictionary format
    raw_data_dict = resp.json()
    print(raw_data_dict)
    data = raw_data_dict['sensors'][0]['data']['Plates In']
    data_new = pd.DataFrame()
    for i in range(len(data)):
        # data_new.loc[i, 'Timestamp'] = convert_timestamp_to_datetime(data[i].get('Timestamp', ''))
        data_new.loc[i, 'Timestamp'] = data[i].get('Timestamp', '')
        data_new.loc[i, 'Value'] = data[i].get('Value', '')
        print(data_new.loc[i, 'Value'])
        # print('we are done : ', i)

    print(data_new.loc['Timestamp'])
    return data_new

def convert_timestamp_to_datetime(timestamp):
    try:
        # Assuming the timestamp is in seconds (adjust if it's in milliseconds or microseconds)
        timestamp_in_seconds = int(timestamp)

        # Convert the timestamp to a datetime object
        dt_object = pd.to_datetime(timestamp_in_seconds, unit='ms')

        # Format the datetime object as a string
        formatted_datetime = dt_object.strftime("%Y-%m-%d %H:%M:%S")

        return formatted_datetime
    except Exception as e:
        print(f"Error converting timestamp to datetime: {e}")
        return None

if __name__ == '__main__':
    # Target URL
    url = "http://uoweb3.ncl.ac.uk/api/v1.1/sensors/PER_NE_CAJT_NCA189_SJB1_SJB2" \
          "/data/json/?starttime=20230601&endtime=20230830"

    # Request data from Urban Observatory Platform
    resp = requests.get(url)

    # Convert response(Json) to dictionary format
    raw_data_dict = resp.json()

    data = raw_data_dict['sensors'][0]['data']['Plates In']
    data_new = pd.DataFrame()
    for i in range(len(data)):
        data_new.loc[i, 'Value'] = data[i].get('Value', '')
        data_new.loc[i, 'Timestamp'] = data[i].get('Timestamp', '')
        # print('we are done : ', i)
    print('result:\n',data_new)


    # print(raw_data_dict['sensors'][0]['data']['Plates In'][0]['Value'])
    # print(raw_data_dict['sensors'][0]['data']['Plates In'][0]['Timestamp'])
    # for pm in raw_data_dict:

    # print("value:",raw_data_dict["Value"])
    # print("Timestamp:",raw_data_dict["Timestamp"])
