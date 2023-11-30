import boto3
import pandas as pd
import datetime
import json
from Database import *


def put_anomaly_data(aggregate_data):
    anomaly_table = get_table('anomaly_data')

    # retrieve alert values from config file
    temp_avg_max, temp_alert_threshold, heartrate_avg_min, heartrate_alert_threshold, spo2_avg_min, spo2_alert_threshold = read_config()

    temp_alert_count = 0
    heartrate_alert_count = 0
    spo2_alert_count = 0

    # convert the aggregate table values into dataframe
    df = pd.DataFrame(aggregate_data['Items'])

    for i in df.index:
        if df['datatype'][i] == 'Temperature':
            if float(df['avg'][i]) > temp_avg_max:
                if temp_alert_count < temp_alert_threshold:
                    temp_alert_count += 1
                else:
                    data = {}
                    data['deviceid'] = str(df['deviceid'][i]) + str(df['timestamp'][i])
                    data['deviceid1'] = str(df['deviceid'][i])
                    data['datatype'] = str(df['datatype'][i])
                    data['timestamp'] = str(df['timestamp'][i])
                    data['alert_message'] = "Temperature anomaly detected!"
                    anomaly_table.put_item(Item=data)
                    temp_alert_count = 0

        if df['datatype'][i] == 'HeartRate':
            if float(df['avg'][i]) < heartrate_avg_min:
                if heartrate_alert_count < heartrate_alert_threshold:
                    heartrate_alert_count += 1
                else:
                    data = {}
                    data['deviceid'] = str(df['deviceid'][i]) + str(df['timestamp'][i])
                    data['deviceid1'] = str(df['deviceid'][i])
                    data['datatype'] = str(df['datatype'][i])
                    data['timestamp'] = str(df['timestamp'][i])
                    data['alert_message'] = "HeartRate anomaly detected!"
                    anomaly_table.put_item(Item=data)
                    heartrate_alert_count = 0

        if df['datatype'][i] == 'SPO2':
            if float(df['avg'][i]) < spo2_avg_min:
                if spo2_alert_count < spo2_alert_threshold:
                    spo2_alert_count += 1
                else:
                    data = {}
                    data['deviceid'] = str(df['deviceid'][i]) + str(df['timestamp'][i])
                    data['deviceid1'] = str(df['deviceid'][i])
                    data['datatype'] = str(df['datatype'][i])
                    data['timestamp'] = str(df['timestamp'][i])
                    data['alert_message'] = "SPO2 anomaly detected!"
                    anomaly_table.put_item(Item=data)
                    spo2_alert_count = 0


def read_config():
    # default values in case config files dont contain any alert values
    temp_avg_max = 105
    temp_alert_threshold = 1
    heartrate_avg_min = 85
    heartrate_alert_threshold = 1
    spo2_avg_min = 95
    spo2_alert_threshold = 1

    # read the config json file
    f = open("config.json")
    config = json.loads(f.read())
    f.close()

    # parse the rules in the json file for alert values, if any
    for item in config['rules']:
        if item['type'] == 'Temperature':
            temp_avg_max = item['avg_max']
            temp_alert_threshold = item['alert_threshold']

        if item['type'] == 'HeartRate':
            heartrate_avg_min = item['avg_min']
            heartrate_alert_threshold = item['alert_threshold']

        if item['type'] == 'SPO2':
            spo2_avg_min = item['avg_min']
            spo2_alert_threshold = item['alert_threshold']

    return temp_avg_max, temp_alert_threshold, heartrate_avg_min, heartrate_alert_threshold, spo2_avg_min, spo2_alert_threshold
