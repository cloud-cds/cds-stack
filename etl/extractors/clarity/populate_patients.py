# populate_patients.py
import sys
import os
from resources import DBLink
from dashan_config import Config




if __name__ == '__main__':
  instance = sys.argv[1]
  config = Config(instance)
  dblink_id = instance
  dblink = DBLink(config)
  dblink.connect()
  # import patient encounters
  dblink.import_patients(dblink_id)
  dblink.disconnect()