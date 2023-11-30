import boto3
import pandas as pd
import datetime
from Database import *


def get_raw_data():
    raw_table = get_table('raw_data')
    raw_data = raw_table.scan()
    return raw_data
