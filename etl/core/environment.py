"""
All environment variables should be loaded from here.
This makes it easy to manage state.
"""
import os
from etl.core.singleton import Singleton

@Singleton
class Environment:
  def __init__(self):
    '''
    All flags will be named the same as the environment variable unless specified.

    Tuple format:
    ============================================================================
    | ENVIRONMENT VAR  |  TYPE  |  DEFAULT  |  DESCRIP  |  NEW NAME (optional) |
    ============================================================================

    Possible types: {string, integer, float, boolean}
    '''

    self.all_vars = [
      # Database connection flags
      ("db_host",       "string",  "db.dev.opsdx.io", "Database host name."),
      ("db_port",       "integer", 5432, "Database port number."),
      ("db_name",       "string",  "opsdx_dev", "Database name."),
      ("db_user",       "string",  "opsdx_root", "Database user name."),
      ("db_password",   "string",  None, "Database password."),

      # Epic2op flags
      ("etl_graph",     "string",  "etl_graph", "Filename to save the etl time graph as."),

      # AWS flags
      ("AWS_ACCESS_KEY_ID",     "string", None,         "Access key for AWS account"),
      ("AWS_SECRET_ACCESS_KEY", "string", None,         "Secret key for AWS account"),
      ("AWS_DEFAULT_REGION",    "string", "us-east-1",  "AWS region"),
      ("SUPPRESS_CLOUDWATCH",   "boolean", False,       "Prevents cloudwatch logging"),

      # JHAPI flags
      ("TREWS_ETL_EPIC_NOTIFICATIONS", "integer",  0,   "Whether to send notifications to Epic."),
      ("JHAPI_SEMAPHORE",              "integer",  50,  "Number of simultaneous connections allowed to jhapi."),
      ("JHAPI_BACKOFF_BASE",           "integer",  2,   "The base of backoff function"),
      ("JHAPI_BACKOFF_MAX",            "integer",  60,  "The maximum backoff seconds"),
      ("JHAPI_ATTEMPTS_SESSION",       "integer",  5,   "The number of retry attempts for a session"),
      ("JHAPI_ATTEMPTS_REQUEST",       "integer",  5,   "The number of retry attempts for a URL request"),

      # ETL flags
      ("ETL_NAME",  "string", None,   "ETL Namespace"),
    ]
    self.set_vars(self.all_vars)



  def set_vars(self, vars):
    ''' Set all the variables '''
    for v in vars:
      var_name, var_type, var_default = v[:3]
      value = os.getenv(var_name, var_default)
      if var_type == "string":
        setattr(self, var_name, str(value))
      elif var_type == "integer":
        setattr(self, var_name, int(value))
      elif var_type == "float":
        setattr(self, var_name, float(value))
      elif var_type == "boolean":
        setattr(self, var_name, value in ["True", "T", "true", "t", "1", "Yes", "Y", "yes", "y", 1])
      else:
        raise ValueError("Type {} not supported".format(v[1]))



  def display(self):
    ''' Display all variables '''
    print("Environment variables:")
    print("  {:15} {:9} {:18} {:20}".format("Name", "Type", "Default", "Description"))
    print("  {:15} {:9} {:18} {:20}".format("----", "----", "-------", "-----------"))
    for var in self.all_vars:
      print("  {:15} {:9} {:18} {:20}".format(*[str(v) if v else '' for v in var]))
