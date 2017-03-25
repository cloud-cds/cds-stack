import sys
import os
from resources import DBLink
from dashan_config import Config




if __name__ == '__main__':
  dblink_id = sys.argv[1]
  config = Config(dblink_id)
  dblink = DBLink(config)
  dblink.connect()
  dblink.create_dblink(dblink_id)
  dblink.disconnect()