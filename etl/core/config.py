'''
Global variables
'''
import os
import logging
import logging.config

TIMEZONE = 'US/Eastern'
tsp_fmt = '%Y-%m-%d %H:%M:%S %Z'
LOG_FMT = '%(asctime)s|%(name)s|%(pid)s|%(levelname)s|%(message)s'

class Config:
    """
    Configuration for dashan database connection
    """
    CONF = os.path.dirname(os.path.abspath(__file__))
    CONF = os.path.join(CONF, 'conf')
    FEATURE_MAPPING_CSV = os.path.join(CONF, 'feature_mapping.csv')

    def set_log(self, log):
        self.log = log

    def __init__(self, name='Dashan', debug=False, logfile=None, conf=None, db_name=None, db_host=None, dataset_id=None):
        self.log_fmt = LOG_FMT
        if conf:
            self.CONF = conf
            # self.LOG_CONF = os.path.join(self.CONF, 'logging.conf')
            self.FEATURE_MAPPING_CSV = os.path.join(self.CONF, 'feature_mapping.csv')
        if dataset_id is not None:
            self.dataset_id = dataset_id
        if logfile:
            self.log = logging.getLogger(name)
            fh = logging.FileHandler(logfile)
            formatter = logging.Formatter(self.log_fmt)
            fh.setFormatter(formatter)
            self.log.addHandler(fh)
        else:
            level = logging.INFO
            # print(log)
            self.log_fmt = self.log_fmt.replace("%(name)s", name)
        if debug:
            level = logging.DEBUG
        # print(self.log_fmt)
        self.log_fmt = self.log_fmt.replace("%(pid)s", str(os.getpid()))
        # print(self.log_fmt)
        self.log = logging.getLogger(name)
        self.log.setLevel(level)
        sh = logging.StreamHandler()
        formatter = logging.Formatter(self.log_fmt)
        sh.setFormatter(formatter)
        self.log.addHandler(sh)
        self.log.propagate = False
        # logging.basicConfig(level    = level,
        #                     format   = self.log_fmt,
        #                     handlers = [logging.StreamHandler()])
        logging.info(self.log_fmt)
        # self.log = logging

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
        # self.log.info("current database: {} at {}".format(self.db_name, self.db_host))

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
