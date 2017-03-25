# populate_features_in_parallel.py
import sys
import os
from populate_cdm import populate_cdm_in_parallel

"""
parameters:
@1: instance name
@2: number of processes
@3: debug on?
@4: plan on?
"""
if __name__ == '__main__':
    instance = sys.argv[1]
    num_procs = int(sys.argv[2])
    is_debug = sys.argv[3].lower() == 'true'
    is_plan = sys.argv[4].lower() == 'true'
    log_path = os.path.dirname(__file__)
    folder = os.path.join(log_path, instance, 'populate_log')
    print "populating log:", folder
    if not os.path.exists(folder):
        os.makedirs(folder)
    populate_cdm_in_parallel(instance, debug=is_debug, log_folder=folder, \
        nprocs= 10, plan=is_plan)
