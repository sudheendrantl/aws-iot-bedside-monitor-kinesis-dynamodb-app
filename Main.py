import boto3
import pandas as pd
import datetime
import time
from RawDataModel import *
from AggregateModel import *
from AlertDataModel import *

while True:

    try:

        # get raw data
        raw_data = get_raw_data()
        print(raw_data)

        # generate and write aggregated data
        put_agg_data(raw_data)

        # detect and write anomaly data
        put_anomaly_data(get_agg_data())

        # wait for some time before we do the above steps again!
        time.sleep(5)

    except Exception as e:
        print(e)
        exit(0)
