from sshtunnel import SSHTunnelForwarder
import fire
import os

def main(namespace, dataset_id, ssh_pkey='/home/nfinkel1/keys/tf-opsdx',
         host='dw.jh.opsdx.io', datadir='/home/nfinkel1/mnt/rambo/'):

    with SSHTunnelForwarder(('controller.jh.opsdx.io', 22),
                        ssh_username='ubuntu',
                        ssh_pkey=ssh_pkey,
                        remote_bind_address=(host, 5432),
                        local_bind_address=('127.0.0.1', 63334)):

        dump = "pg_dump -h {h} -U opsdx_root --schema={n} opsdx_dev_dw > {d}{n}.sql"
        os.system(dump.format({'h': host, 'n': namespace, 'd': datadir}))
        

	conn = 'dbname=cardiac_db_small user=opsdx_root host=127.0.0.1 port=63334'
	conn = psycopg2.connect(conn)
	cur = conn.cursor()

        with open('{}{}.sql'.format(datadir, namespace), 'r') as f:
	    query = f.read()

        cur.execute(query)



        os.system('python ../etl/clarity2dw/planner.py')



if __name__ == '__main__':
    fire.Fire(main)
