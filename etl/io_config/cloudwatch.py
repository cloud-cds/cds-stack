import boto3, botocore
from etl.core.environment import Environment
import datetime as dt
import logging

class Cloudwatch:
  def __init__(self):
    flags = Environment()
    self.client = boto3.client('cloudwatch',
      region_name           = flags.AWS_DEFAULT_REGION,
      aws_access_key_id     = flags.AWS_ACCESS_KEY_ID,
      aws_secret_access_key = flags.AWS_SECRET_ACCESS_KEY,)
    self.prod_or_dev = flags.db_name.split('_')[1]


  def build_metric_data(self, dimension_name, metric_name, value, unit='None'):
    ''' Build a dictionary of the correct type for cloudwatch metrics '''
    metric_data = {
      'MetricName': metric_name,
      'Value':      value,
      'Unit':       unit,
      'Dimensions': [{'Name': dimension_name, 'Value': self.prod_or_dev}],
      'Timestamp':  dt.datetime.utcnow(),
    }
    return metric_data


  def push(self, dimension_name, metric_name, value, unit='None'):
    ''' Push a metric to cloudwatch '''

    metric_data = [build_metric_data(dimension_name, metric_name, value, unit)]

    try:
      self.client.put_metric_data(Namespace='OpsDX', MetricData=metric_data)
      logging.info('Successfully pushed {} to cloudwatch'.format(metric_name))
    except botocore.exceptions.EndpointConnectionError as e:
      logging.error('Unsuccessfully pushed {} to cloudwatch'.format(metric_name))
      logging.error(e)



  def push_many(self, dimension_name, metric_names, metric_values, metric_units):
    ''' Push a list of metrics to cloudwatch '''

    # Check all inputs
    if type(metric_names) != list or type(values) != list or type(units) != list:
      raise TypeError('metric_names, values, and units must be of type list')
    if len(metric_names) != len(values) or len(values) != len(units):
      raise ValueError('lists must be of same length')

    # Build metric data
    metric_data = []
    for idx, metric in enumerate(metric_names):
      metric_data.append(
        build_metric_data(dimension_name, metric, values[idx], units[idx])
      )

    # Push to cloudwatch
    try:
      self.client.put_metric_data(Namespace='OpsDX', MetricData=metric_data)
      logging.info('Successfully pushed to cloudwatch')
    except botocore.exceptions.EndpointConnectionError as e:
      logging.error('Unsuccessfully pushed to cloudwatch')
      logging.error(e)

