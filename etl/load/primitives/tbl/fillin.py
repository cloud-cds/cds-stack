import sys

##############
# fillin pipeline
##############
def fillin(fid, fillin_func_id, fillin_func_args):
  this_mod = sys.modules[__name__]
  func = getattr(this_mod, fillin_func_id)
  return func(fid, fillin_func_args)

def last_value_in_window(fid, args):
  table = args[0]
  win_h = int(args[1])
  recalculate_popmean = args[2]
  dataset_id = args[3]
  if dataset_id is not None:
    return '''
              select last_value_in_window('%s', '%s', %s, %s, %s);
              ''' % (fid, table, win_h, recalculate_popmean, dataset_id)
  else:
    return '''
              select last_value_in_window('%s', '%s', %s, %s);
              ''' % (fid, table, win_h, recalculate_popmean)