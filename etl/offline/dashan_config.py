"""
config.py
"""
import os
import logging
import logging.config
import ConfigParser
class Config:
  """
  Configuration for dashan database connection
  """
  CONF = os.path.dirname(os.path.abspath(__file__))
  CONF = os.path.join(CONF, 'conf')
  LOG_CONF = os.path.join(CONF, 'logging.conf')
  DATALINK_DIR = os.path.join(CONF, 'datalink')
  CDM_FEATURE_CSV = os.path.join(CONF, 'CDM_Feature.csv')
  CDM_FUNCTION_CSV = os.path.join(CONF, 'CDM_Function.csv')

  LOG_FMT = '%(asctime)s|%(name)s|%(levelname)s|%(message)s'

  def set_log(self, log):
    self.log = log

  def __init__(self, dbname, log='Dashan', debug=False, logfile=None):
    if logfile:
      self.log = logging.getLogger(log)
      fh = logging.FileHandler(logfile)
      formatter = logging.Formatter(self.LOG_FMT)
      fh.setFormatter(formatter)
      self.log.addHandler(fh)
    else:
      level = logging.INFO
      self.LOG_FMT = self.LOG_FMT.replace("%(name)s", log)
      if debug:
        level = logging.DEBUG
      logging.basicConfig(level=level,
                format=self.LOG_FMT,
                handlers=[logging.StreamHandler()])
      self.log = logging
    # Load settings from config files
    # Load dashan instance config
    self.dashan_conf = os.path.join(self.CONF, '%s.conf' % dbname)

    self.log.info("loading dashan config file:%s" % self.dashan_conf)
    self.config_parser = ConfigParser.ConfigParser()
    self.config_parser.read(self.dashan_conf)

    # Load datalinks
    datalinks_conf = self.config_section_map("DATALINK")["datalink_id"]
    self.log.info("datalinks: %s" % datalinks_conf)
    self.datalinks = datalinks_conf.split(",")

    # Load each datalink
    for dl in self.datalinks:
      self.log.info("loading datalink config for %s" % dl)

    # Load PostgreSQL connection settings
    db_config = self.config_section_map("Database")

    if db_config['user']:
      self.db_user = db_config['user']
    else:
      self.db_user = os.environ['db_user']

    if db_config['host']:
      self.db_host = db_config['host']
    else:
      self.db_host = os.environ['db_host']

    if db_config['port']:
      self.db_port = db_config['port']
    else:
      self.db_port = os.environ['db_port']

    if db_config['password']:
      self.db_pass = db_config['password']
    else:
      self.db_pass = os.environ['db_password']

    if db_config['name']:
      self.db_name = db_config['name']
    else:
      self.db_name = os.environ['db_name']
    self.log.info("current database: %s at %s" % (self.db_name, self.db_host))

  def config_section_map(self, section):
    self.log.info("loading config section: %s" % section)
    dict1 = {}
    options = self.config_parser.options(section)
    for option in options:
      try:
        dict1[option] = self.config_parser.get(section, option)
        if dict1[option] == -1:
          self.log.warning(\
            "Config warning: skip option %s" % option)
        else:
          self.log.info("loading config option: %s" % option)
      except:
        self.log.error("Config exception on %s" % option)
    return dict1

  def get_db_conn_string(self):
    return "user=%s, dbname=%s, host=%s, port=%s, password=%s" \
      % (self.db_user, self.db_name, self.db_host, self.db_port, self.db_pass)

  def get_db_conn_string_sqlalchemy(self):
    if self.db_host:
      return 'postgresql://{}:{}@{}:{}/{}'.format(self.db_user, self.db_pass, self.db_host, self.db_port, self.db_name)
    else:
      return 'postgresql:///{}'.format(self.db_name)