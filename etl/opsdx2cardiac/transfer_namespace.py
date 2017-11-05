from sshtunnel import SSHTunnelForwarder

import psycopg2
import fire
import os


def main(namespace,
         dataset_id,
         cardiac_password,
         opsdx_password,
         ssh_pkey='/home/nfinkel1/keys/tf-opsdx',
         host='dw.jh.opsdx.io',
         datadir='/home/nfinkel1/mnt/rambo/',
         nprocs="8"):

    with SSHTunnelForwarder(
        ('controller.jh.opsdx.io', 22),
            ssh_username='ubuntu',
            ssh_pkey=ssh_pkey,
            remote_bind_address=(host, 5432),
            local_bind_address=('127.0.0.1', 63334)):

        os.environ['PGPASSWORD'] = opsdx_password
        dump = "pg_dump -h {h} -U opsdx_root --schema={n} -d opsdx_dev_dw > {d}{n}.dump"
        os.system(dump.format_map({'h': host, 'n': namespace, 'd': datadir}))

        os.environ['PGPASSWORD'] = cardiac_password
        restore = "pg_restore -h {h} -U opsdx_root -d cardiac_db_small {d}{n}.dump"
        os.system(restore.format_map({'h': host, 'n': namespace, 'd': datadir}))
		
        os.environ['db_password'] = cardiac_password
        os.environ['clarity_workspace'] = namespace
        os.environ['dataset_is'] = str(dataset_id)
        os.environ['nprocs'] = nprocs
        os.environ['num_derive_groups'] = "8"

        os.environ['min_tsp'] = '1990-01-01'
        os.environ['vacuum_temp_table'] = 'True'
        os.environ['db_host'] = 'dw.jh.opsdx.io'
        os.environ['db_port'] = '5432'
        os.environ['db_name'] = 'cardiac_db_small'

        os.environ['feature_mapping'] = "feature_mapping.csv,feature_mapping_cardiac.csv"

        os.environ['offline_criteria_processing'] = 'False'
        os.environ['extract_init'] = 'True'
        os.environ['populate_patients'] = 'True'
        os.environ['fillin'] = 'True'
        os.environ['derive'] = 'True'

        os.system('python ../etl/clarity2dw/planner.py')


if __name__ == '__main__':
    fire.Fire(main)
