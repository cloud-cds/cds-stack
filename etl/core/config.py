'''
Global variables
'''
import os
import logging
import logging.config

# Timestamp format to use everywhere
est_tsp_fmt = '%Y-%m-%dT%H:%M:%S-05:00'

class Config:
    """
    Configuration for dashan database connection
    """
    CONF = os.path.dirname(os.path.abspath(__file__))
    CONF = os.path.join(CONF, 'conf')
    LOG_CONF = os.path.join(CONF, 'logging.conf')
    # may not useful so comment out by zad
    # CDM_FEATURE_CSV = os.path.join(CONF, 'CDM_Feature.csv')
    # CDM_FUNCTION_CSV = os.path.join(CONF, 'CDM_Function.csv')
    FEATURE_MAPPING_CSV = os.path.join(CONF, 'feature_mapping.csv')
    LOG_FMT = '%(asctime)s|%(name)s|%(levelname)s|%(message)s'

    def set_log(self, log):
        self.log = log

    def __init__(self, log='Dashan', debug=False, logfile=None, conf=None, db_name=None, db_host=None, dataset_id=None):
        if conf:
            self.CONF = conf
            self.LOG_CONF = os.path.join(self.CONF, 'logging.conf')
            self.FEATURE_MAPPING_CSV = os.path.join(self.CONF, 'feature_mapping.csv')
        if dataset_id is not None:
            self.dataset_id = dataset_id
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
        logging.basicConfig(level    = level,
                            format   = self.LOG_FMT,
                            handlers = [logging.StreamHandler()])
        self.log = logging

        self.db_user = os.environ['db_user']
        self.db_host = os.environ['db_host']
        self.db_port = os.environ['db_port']
        self.db_pass = os.environ['db_password']
        if db_name:
            self.db_name = db_name
        else:
            self.db_name = os.environ['db_name']
        if db_host:
            self.db_host = db_host
        else:
            self.db_host = os.environ['db_host']
        self.log.info("current database: {} at {}".format(self.db_name, self.db_host))

    def get_db_conn_string(self):
        return "user={}, dbname={}, host={}, port={}, password={}".format(
            self.db_user,
            self.db_name,
            self.db_host,
            self.db_port,
            self.db_pass
        )

    def get_db_conn_string_sqlalchemy(self):
        if self.db_host:
            return 'postgresql://{}:{}@{}:{}/{}'.format(
                self.db_user,
                self.db_pass,
                self.db_host,
                self.db_port,
                self.db_name
            )
        else:
            return 'postgresql:///{}'.format(self.db_name)
