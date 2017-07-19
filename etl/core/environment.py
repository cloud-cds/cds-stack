"""
All environment variables should be loaded from here.
This makes it easy to manage state.
"""
import os

class Environment:
  def __init__(self):
    '''
    All flags will be named the same as the environment variable unless specified.

    Tuple format:
      ENVIRONMENT VAR NAME,   TYPE,   DEFAULT,   DESCRIPTION,   NEW NAME (optional)

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
        setattr(self, var_name, bool(value))
      else:
        raise ValueError("Type {} not supported".format(v[1]))



  def display(self):
    ''' Display all variables '''
    print("Environment variables:")
    print("  {:15} {:9} {:18} {:20}".format("Name", "Type", "Default", "Description"))
    print("  {:15} {:9} {:18} {:20}".format("----", "----", "-------", "-----------"))
    for var in self.all_vars:
      print("  {:15} {:9} {:18} {:20}".format(*[str(v) if v else '' for v in var]))
