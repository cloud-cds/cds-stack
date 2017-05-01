from ml.client import Session
from ml.dashan_input import InputParamFactory
import json
import sys


# ----------------------------------------------------------------
## save report name
# ----------------------------------------------------------------
def save_report(inputValues):

  #--------------------------------------
  ## Build JSON Objects
  #--------------------------------------
  json_dict = {}
  json_dict['inputs'] = inputValues.to_json_dict()


  #--------------------------------------
  ## Save Reports
  #--------------------------------------
  session = Session(inputValues.name)
  print(session)
  session.log.info('connect to the database')
  session.connect()
  session.log.info('saving report ...')
  session.insert_report(json.dumps(json_dict))
  session.log.info('report saved')
  session.disconnect()
  session.log.info('disconnect to the database')

if __name__ == "__main__":
    input_arg = sys.argv[1]

    inputFact = InputParamFactory()
    inputValues = inputFact.parseInput(input_arg)
    save_report(inputValues)
