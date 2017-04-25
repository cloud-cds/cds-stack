from ml.client import Session
import json

def save_report(report):
  session = Session(name)
  session.log.info('connect to the database')
  session.connect()
  session.log.info('saving report ...')
  session.insert_report(json.dumps(report))
  session.log.info('report saved')
  session.disconnect()
  session.log.info('disconnect to the database')