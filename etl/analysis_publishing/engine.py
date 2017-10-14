import etl.analysis_publishing.utils as utils
import etl.analysis_publishing.metrics as metrics
import argparse
import os
import boto3
from datetime import datetime, timedelta
import logging


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.debug("Import complete")

print(os.environ)

def_stack_to_english_dict = {
  'opsdx-prod'    : 'Prod',
  'opsdx-jh-prod' : 'Prod',
  'opsdx-dev'     : 'Dev',
  'opsdx-jh-dev'  : 'Dev',
  'Test'          : 'Test'
}

class Engine:
  def __init__(self, time_start, time_end):
    self.time_start = time_start
    self.time_end = time_end
    out_tsp_fmt, tz = utils.get_tz_format(tz_in_str='US/Eastern')
    self.time_start_str = utils.to_tz_str(self.time_start, out_tsp_fmt, tz)
    self.time_end_str = utils.to_tz_str(self.time_end, out_tsp_fmt, tz)
    self.boto_ses_client = boto3.client('ses')
    self.boto_cloudwatch_client = boto3.client('cloudwatch')

    def try_to_read_from_environ(var_str, default_val):
      if var_str in os.environ:
        logger.info("Selecting {} from Environment".format(var_str))
        return os.environ[var_str]
      else:
        logger.info("Selecting default value for {}".format(var_str))
        return default_val

    self.BEHAMON_STACK = try_to_read_from_environ('BEHAMON_STACK','Test')
    self.receiving_email_address = try_to_read_from_environ('REPORT_RECEIVING_EMAIL_ADDRESS','trews-jhu@opsdx.io')


  def run(self, mode):
    ''' Run the behavioral monitor engine '''
    self.engine = utils.get_db_engine()
    logger.info("engine created")
    self.connection = self.engine.connect()
    logger.info("connection made")


    if mode == 'reports':
      metric_list = [
        metrics.report_introduction,
        metrics.pats_seen_by_docs,
        metrics.user_engagement,
        metrics.trews_but_no_cms_stats,
        metrics.cms_but_no_trews_stats,
        metrics.trews_and_cms_stats,
        metrics.suspicion_of_infection_modified,
        # metrics.get_sepsis_state_stats,
        metrics.notification_stats,
        metrics.pats_with_threshold_crossings
      ]

    elif mode == 'metrics':
      metric_list = [
        metrics.unique_usrs,
        metrics.get_sepsis_state_stats,
        metrics.pats_with_threshold_crossings,
      ]

    else:
      logger.error("Invalid mode: {}".format(mode))

    report_metric_factory = metrics.metric_factory(self.connection,self.time_start_str, self.time_end_str, metric_list)
    report_metric_factory.calc_all_metrics()

    if mode == 'reports':
      report_html_body = report_metric_factory.build_report_body()
      self.send_email(report_html_body)
    elif mode == 'metrics':
      cwm_metrics_list = report_metric_factory.get_cwm_output()
      self.push_to_cwm(cwm_metrics_list)
    else:
      logger.error("Invalid mode: {}".format(mode))

    self.connection.close()
    self.engine.dispose()


  def push_to_cwm(self, cwm_list):
    logger.info("Sending the following metrics to cloudwatch:")

    for x in cwm_list:
      logger.info("\t" + str(x))

    for md in cwm_list:
      md['Dimensions'] = [{'Name': 'analysis','Value': self.BEHAMON_STACK}]

    put_status = self.boto_cloudwatch_client.put_metric_data(
      Namespace  = 'OpsDX',
      MetricData = cwm_list,
    )

    logger.info("Metrics sent to cloudwatch ")


  def send_email(self, build_report_body):
    ''' Build and send an email for the report metrics data '''
    logger.info('Sending report email')

    # print("email sent just to peter")

    self.boto_ses_client.send_email(
      Source='trews-jhu@opsdx.io',
      Destination={
        'ToAddresses': [self.receiving_email_address],
      },
      Message={
        'Subject': {'Data': 'Report Metrics (%s)' % def_stack_to_english_dict[self.BEHAMON_STACK]},
        'Body': {
          'Html': {'Data': build_report_body},
        },
      }
    )

    logger.info('email sent')




def parse_arguments():
  parser = argparse.ArgumentParser(description='Behavioral monitoring engine')
  parser.add_argument('mode', type=str, choices=['reports', 'metrics'])
  parser.add_argument('execution_period_minutes', type=int)
  return parser.parse_args()




if __name__ == '__main__':
  args = parse_arguments()
  last_execution = datetime.now() - timedelta(minutes=args.execution_period_minutes)
  this_execution = datetime.now()
  engine = Engine(last_execution, this_execution)
  engine.run(args.mode)
