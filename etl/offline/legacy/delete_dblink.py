"""
delete_dblink.py
an example to delete dblink
"""
import sys
import os
from resources import DBLink
from dashan_config import Config




if __name__ == '__main__':
  dblink_id = sys.argv[1]
  config = Config(dblink_id)
  dblink = DBLink(config)
  dblink.connect()
  dblink.delete(dblink_id)
  dblink.disconnect()