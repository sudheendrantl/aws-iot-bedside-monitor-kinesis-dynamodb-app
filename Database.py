import boto3
import pandas as pd
import datetime

def get_table(table_name):
    db = boto3.resource('dynamodb', region_name='us-east-1')
    return db.Table(table_name)
