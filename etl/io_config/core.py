import os

def get_environment_var(variable_name, default_value=None):
  if variable_name in os.environ:
    return os.environ[variable_name]
  elif default_value is None:
    raise KeyError("""Either an environment variable for "{}" needs to be set
      or the value needs to be passed into the constructor.
      """.format(variable_name))
  else:
    return default_value
