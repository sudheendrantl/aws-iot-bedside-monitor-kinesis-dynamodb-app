import boto3
import pandas as pd
import datetime
from Database import *


def get_agg_data():
    agg_table = get_table('aggregate_data')
    agg_data = agg_table.scan()
    return agg_data


def put_agg_data(raw_data):
    agg_table = get_table('aggregate_data')

    # convert the table values into dataframe
    df = pd.DataFrame(raw_data['Items'])
    df['timestamp1'] = df['timestamp'].apply(
        lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').replace(second=0, microsecond=0))

    h1 = df[(df['datatype'] == 'HeartRate') & (df['deviceid'] == 'BSM_G101')]
    s1 = df[(df['datatype'] == 'SPO2') & (df['deviceid'] == 'BSM_G101')]
    t1 = df[(df['datatype'] == 'Temperature') & (df['deviceid'] == 'BSM_G101')]

    h2 = df[(df['datatype'] == 'HeartRate') & (df['deviceid'] == 'BSM_G102')]
    s2 = df[(df['datatype'] == 'SPO2') & (df['deviceid'] == 'BSM_G102')]
    t2 = df[(df['datatype'] == 'Temperature') & (df['deviceid'] == 'BSM_G102')]

    h1g = h1.groupby('timestamp1').agg({'value': ['mean', 'min', 'max']})
    s1g = s1.groupby('timestamp1').agg({'value': ['mean', 'min', 'max']})
    t1g = t1.groupby('timestamp1').agg({'value': ['mean', 'min', 'max']})
    h2g = h2.groupby('timestamp1').agg({'value': ['mean', 'min', 'max']})
    s2g = s2.groupby('timestamp1').agg({'value': ['mean', 'min', 'max']})
    t2g = t2.groupby('timestamp1').agg({'value': ['mean', 'min', 'max']})

    for i, row in h1g.iterrows():
        data = {}
        data['deviceid'] = 'BSM_G101' + 'HeartRate'
        data['deviceid1'] = 'BSM_G101'
        data['datatype'] = 'HeartRate'
        data['timestamp'] = datetime.datetime.strftime(i, '%Y-%m-%d %H:%M')

        values = []
        for item in row:
            values.append(item)

        data['avg'] = str(values[0])
        data['min'] = values[1]
        data['max'] = values[2]
        print(data)
        agg_table.put_item(Item=data)

    for i, row in s1g.iterrows():
        data = {}
        data['deviceid'] = 'BSM_G101' + 'SPO2'
        data['deviceid1'] = 'BSM_G101'
        data['datatype'] = 'SPO2'
        data['timestamp'] = datetime.datetime.strftime(i, '%Y-%m-%d %H:%M')

        values = []
        for item in row:
            values.append(item)

        data['avg'] = str(values[0])
        data['min'] = values[1]
        data['max'] = values[2]
        print(data)
        agg_table.put_item(Item=data)

    for i, row in t1g.iterrows():
        data = {}
        data['deviceid'] = 'BSM_G101' + 'Temperature'
        data['deviceid1'] = 'BSM_G101'
        data['datatype'] = 'Temperature'
        data['timestamp'] = datetime.datetime.strftime(i, '%Y-%m-%d %H:%M')

        values = []
        for item in row:
            values.append(item)

        data['avg'] = str(values[0])
        data['min'] = values[1]
        data['max'] = values[2]
        print(data)
        agg_table.put_item(Item=data)

    for i, row in h2g.iterrows():
        data = {}
        data['deviceid'] = 'BSM_G102' + 'HeartRate'
        data['deviceid1'] = 'BSM_G102'
        data['datatype'] = 'HeartRate'
        data['timestamp'] = datetime.datetime.strftime(i, '%Y-%m-%d %H:%M')

        values = []
        for item in row:
            values.append(item)

        data['avg'] = str(values[0])
        data['min'] = values[1]
        data['max'] = values[2]
        print(data)
        agg_table.put_item(Item=data)

    for i, row in s2g.iterrows():
        data = {}
        data['deviceid'] = 'BSM_G102' + 'SPO2'
        data['deviceid1'] = 'BSM_G102'
        data['datatype'] = 'SPO2'
        data['timestamp'] = datetime.datetime.strftime(i, '%Y-%m-%d %H:%M')

        values = []
        for item in row:
            values.append(item)

        data['avg'] = str(values[0])
        data['min'] = values[1]
        data['max'] = values[2]
        print(data)
        agg_table.put_item(Item=data)

    for i, row in t2g.iterrows():
        data = {}
        data['deviceid'] = 'BSM_G102' + 'Temperature'
        data['deviceid1'] = 'BSM_G102'
        data['datatype'] = 'Temperature'
        data['timestamp'] = datetime.datetime.strftime(i, '%Y-%m-%d %H:%M')

        values = []
        for item in row:
            values.append(item)

        data['avg'] = str(values[0])
        data['min'] = values[1]
        data['max'] = values[2]
        print(data)
        agg_table.put_item(Item=data)

    return
