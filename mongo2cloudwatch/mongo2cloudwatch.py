#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# bug-report: makssych@gmail.com
import requests
import datetime
import boto3
from pymongo import MongoClient
_METADATA_URL = "http://169.254.169.254/latest/meta-data"


# Create CloudWatch client
cloudwatch = boto3.client('cloudwatch', region_name='eu-central-1')
curr_metrics = []


def append_metric(metricName, dimensions, val, unit='Count'):
    m_data = {
        'MetricName': metricName,
        'Dimensions': dimensions,
        'Unit': unit,
        'Value': val
        # 'Timestamp': datetime.datetime.now(),
        # 'StatisticValues': {
        #     'SampleCount': val,
        #     'Sum': val,
        #     'Minimum': val,
        #     'Maximum': val
        # }
    }
    print(m_data)
    curr_metrics.append(m_data)


def mongo_status(client):
    db = client.test
    connections_dict = db.command("serverStatus")
    return connections_dict


def mongo_current_conn(c_dict):
    return float(c_dict["connections"]['current'])


def mongo_available_conn(c_dict):
    return float(c_dict["connections"]['available'])


if __name__ == '__main__':
    instance_id = requests.get( _METADATA_URL + '/instance-id').text
    dimensions = [{'Name' : 'MongoDB', 'Value': instance_id}]
    try:
        # connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
        client = MongoClient('localhost', 27017)
        stat_dict = mongo_status(client)
        if len(stat_dict) != 0:
            append_metric('mongo_current_conn', dimensions, val=mongo_current_conn(stat_dict), unit='Count')
            append_metric('mongo_available_conn', dimensions, val=mongo_available_conn(stat_dict), unit='Count')

    finally:
        if client:
            client.close()

    if len(curr_metrics) != 0:
        response = cloudwatch.put_metric_data(MetricData = curr_metrics, Namespace='Services')
        print(response)
