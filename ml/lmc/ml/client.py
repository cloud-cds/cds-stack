# client.py
# provide client API in python to access EMR in the server
import sys, os
import numpy as np
import csv
import datetime
from config import Config
from sqlalchemy import create_engine, text
import pandas as pd

class DataFrameFactory():
    def load_csv(self, fname):
        # array = np.genfromtxt(fname, delimiter=',', skip_header=1,\
        #     converters = {0: self.convert_int, 1:self.convert_time})
        # print array[:10]
        reader = csv.reader(open(fname, 'rb'), delimiter=',')
        text = list(reader)
        header = text[0]
        data = text[1:]
        for row in data:
            row[0] = int(row[0])
            row[1] = self.convert_time(row[1])
            for i in range(2, len(row)):
                if row[i] == "":
                    row[i] = None
                elif row[i] == 'True' or row[i] == 'False':
                    row[i] = bool(row[i])
                else:
                    row[i] = float(row[i])
        array = np.asarray(data)
        return DataFrame(array, header)

    def load(self, fname):
        colname_lst = np.load(fname + ".meta.npy").tolist()
        # print colname_lst
        data = np.load(fname + ".npy")
        return DataFrame(data, colname_lst)

    def convert_int(self, value):
        return int(value)

    def convert_time(self, value):
        return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')

class DataFrame(np.ndarray):
    """ The data frame class contains indices (enc_id, tsp) and feature values
        Also, it is a decoration class to help users to access numpy array
    """
    def __new__(cls, array, feature_name_lst):
        self = array.view(cls)
        self.colname_lst = feature_name_lst
        return self

    def index(feature):
        return self.colname_lst.index(feature)

    def colnames(self):
        return self.colname_lst

    def get_feature_names(self):
        return self.colname_lst[2:]

    def rename_col(self, old, new):
        idx = self.colname_lst.index(old)
        self.colname_lst[idx] = new

    def remove_cols(self, colnames):
        # idx = self.colname_lst.index(colname)
        # self.colname_lst.remove(colname)
        colname_lst = [col for col in self.colname_lst if col not in colnames]
        indices = [self.colname_lst.index(col) for col in colname_lst]
        return DataFrame(self[:,indices], colname_lst)

    def head(self, nrows):
        return DataFrame(self[:nrows,:], self.colname_lst)


    def __getitem__(self, key):
        if type(key) is tuple:
            tup = key
            row, col = tup
            # colname_lst = []
            if isinstance(col, list):
                for i, item in enumerate(col):
                    if isinstance(item, str):
                        col[i] = self.colname_lst.index(item)
                        # colname_lst[i] = item
                    # else:
                    #     colname_lst[i] = self.colname_lst[item]

            else:
                if isinstance(col, str):
                    # colname_lst = [col]
                    col = self.colname_lst.index(col)
            if isinstance(row, np.ndarray):
                return np.ndarray.__getitem__(self, row)[:, col]
            else:
                return np.ndarray.__getitem__(self, (row, col))
        else:
            return np.ndarray.__getitem__(self, key)

    def __setitem__(self, key, value):
        if type(key) is tuple:
            row, col = key
            if isinstance(col, str):
                col = self.colname_lst.index(col)
            elif isinstance(col, list):
                for i, name in enumerate(col):
                    if isinstance(name, str):
                        col[i] = self.colname_lst.index(name)
            np.ndarray.__setitem__(self, (row, col), value)
        else:
            np.ndarray.__setitem__(self, key, value)


    def nrows(self):
        return self.shape[0]

    def select_rows(self, row_idx):
        array = self[row_idx]
        return DataFrame(array, self.colname_lst)

    def savecsv(self, fname):
        # np.savetxt(fname, self, delimiter=',', header=','.join(self.colname_lst))
        # np.savetxt(fname, self)
        with open(fname, 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(self.colname_lst)
            writer.writerows(self)

    def save(self, fname):
        fname = str(fname)
        np.save(fname, self)
        np.save(fname+ ".meta", self.colname_lst)

    def replace_none(self, colname, value):
        idx = self[:, colname] == np.array(None)
        self[idx, colname] = value



class Session():
    def __init__(self, name):
        self.name = name
        self.config = Config()
        self.log = self.config.log
        self.db_conn_string = self.config.get_db_conn_string_sqlalchemy()
        self.engine = create_engine(self.db_conn_string)

    def connect(self):
        self.conn = self.engine.connect()

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def __str__(self):
        this_str = 'dashan/ml session object \n'
        this_str += 'Name : {} \n'.format(self.name)
        return this_str


    """ operations for clients """
    def get_all_patient_encounter_ids(self, filters=None):
        """
        Return all patient encounter ids which have at least one observation
        """
        sql = "SELECT distinct enc_id FROM pat_enc"
        if filters:
            for filter_ in filters:
                sql += " " + filter_
        sql += " order by enc_id"
        self.log.info("sql:")
        self.log.info(sql)
        cursor = self.conn.execute(sql)
        ids = np.array([row['enc_id'] for row in cursor.fetchall()])

        return ids

    def query_feature(self, fid):
        """
        Return all values of fid
        """
        attributes = self.get_feature_attributes([fid])
        if len(attributes) == 0:
            self.log.warn("Feature %s not found." % fid)
        else:
            category = attributes[0]['category']
            results = None
            if category == "S":
                cursor = self.select_s(cols="value, confidence", \
                    where_clause="where fid = '%s'" % fid)
                results = cursor.fetchall()

            elif category == 'T':
                cursor = self.select_t(cols="value, confidence", \
                    where_clause="where fid = '%s'" % fid)
                results = cursor.fetchall()

            elif category == 'TWF':
                cursor = self.select_twf(cols="%(fid)s, %(fid)s_c" % {'fid':fid})
                results = cursor.fetchall()


            return np.asarray(results)

    def query_twf_features(self, features, enc_id=None, nrows=0):
        """
        query values for selected features for selected enc_id (optionally)
        """
        columns = ['enc_id', 'tsp'] + features
        sql = "SELECT %s FROM cdm_twf"\
            % ",".join(columns)
        if nrows > 0:
            sql += " limit %d" % nrows
        if enc_id:
            sql += " where enc_id = %d" % enc_id
        sql += " order by enc_id, tsp"
        self.log.info('query_twf_features:' + sql)
        cursor = self.conn.execute(sql)
        twf_data = cursor.fetchall()

        return np.asarray(twf_data)

    def delete_user_specific_twf(self, feature_name):
        sql = """
        UPDATE cdm_twf SET
        user_specific = json_object_del_key(user_specific, '%s')
        WHERE user_specific is not null
        """ % feature_name
        self.log.info('delete_user_specific_twf:%s' % feature_name)
        self.update_with_sql(self.conn, sql)

    def set_user_specific_twf(self, feature_name, value, enc_id, tsp):
        sql = """
        UPDATE cdm_twf SET
        user_specific = json_object_set_key(
            coalesce(user_specific, '{}'), '%(fn)s', %(fv)s
            )
        WHERE enc_id = %(enc_id)s and tsp = '%(tsp)s'::timestamp without time zone
        """ % {'fn':feature_name, 'fv':value, 'enc_id':enc_id,
               'tsp':tsp.strftime('%Y-%m-%d %H:%M:%S')}
        self.update_with_sql(self.conn, sql)

    def get_feature_data_type(self, features):
        sql = "select fid, data_type from cdm_feature"
        cursor = self.conn.execute(sql)
        feature_type = cursor.fetchall()

        return [row['data_type'] for row in feature_type if row['fid'] in features]


    def query_single_category_feature(self, features, nrows=0):
        """
        query entry-attribute-values for selected single features
        """
        if features is None or len(features) == 0:
            print("features are invalid")
            return
        # data_type = self.get_feature_data_type(features)
        sql = """
        SELECT enc_id, fid, value FROM cdm_s
        where %s
        order by enc_id
        """ % " or ".join(["fid = '%s'" % f for f in features])
        if nrows > 0:
            sql += " limit %d" % nrows
        self.log.info('query_s_features:' + sql)
        cursor = self.conn.execute(sql)
        s_data = cursor.fetchall()

        return s_data

    def get_cdm_feature_list(self, category=None, dataset_id=None):
        sql = "SELECT fid FROM cdm_feature"+ (" where dataset_id = {}".format(dataset_id) if dataset_id else '')
        if category is not None:
            sql += " where category = '%s'" % category
        sql += " order by fid"
        cursor = self.conn.execute(sql)
        features = cursor.fetchall()

        return [row[0] for row in features]


    def get_feature_attributes(self, feature_list=None, dataset_id=None):
        sql = "SELECT * FROM cdm_feature" + (" where dataset_id = {}".format(dataset_id) if dataset_id else '')
        cursor = self.conn.execute(sql)
        all_features = cursor.fetchall()

        if feature_list is None:
            return [row for row in all_features]
        else:
            return [row for row in all_features if row['fid'] in feature_list]

    def select_feature_mapping(self):
        sql = """
        SELECT * FROM dblink_feature_mapping
        INNER JOIN cdm_feature ON dblink_feature_mapping.fid = cdm_feature.fid
        WHERE cdm_feature.is_deprecated = 'f'
        AND cdm_feature.is_measured = 't'
        ORDER BY cdm_feature.fid
        """
        server_cursor = self.conn.execute(sql)
        mapping = server_cursor.fetchall()
        serve
        return mapping

    def cast(self, value, data_type):
        if data_type == 'Real':
            return float(value)
        elif data_type == 'Integer':
            return int(value)
        elif data_type == 'Boolean':
            return bool(value)
        elif data_type == 'String':
            return str(value)
        else:
            print('Error: unknown data type!!!!!!')



    def download_snapshot_for_enc_id(self, enc_id):
        sql = "SELECT * from pat_enc where enc_id = '%s'" % enc_id
        csv_file = "%s.pat_enc.csv" % enc_id
        self.query_to_csv(sql, csv_file)

        sql = "SELECT * from cdm_s where enc_id = '%s'" % enc_id
        csv_file = "%s.cdm_s.csv" % enc_id
        self.query_to_csv(sql, csv_file)

        sql = "SELECT * from cdm_t where enc_id = '%s'" % enc_id
        csv_file = "%s.cdm_t.csv" % enc_id
        self.query_to_csv(sql, csv_file)

        sql = "SELECT * from cdm_twf where enc_id = '%s'" % enc_id
        csv_file = "%s.cdm_twf.csv" % enc_id
        self.query_to_csv(sql, csv_file)

    def download_data_frame(self, feature_lst, nrows=None, where=None,
                            as_data_frame=True):
        sql = self.build_sql_string(feature_lst, nrows=nrows, where=where)
        return self.download_sql_string(sql, feature_lst, as_data_frame = as_data_frame)

    def download_sql_string(self, sql, colnames, as_data_frame=True):
        # self.log.info('query data frame:' + sql)
        cursor = self.conn.execute(sql)
        data = cursor.fetchall()
        if as_data_frame:
            if len(data) == 0:
                return pd.DataFrame(columns=colnames)
            else:
                return pd.DataFrame(np.asarray(data), columns=colnames)
        else:
            return data

    def build_sql_string(self, feature_lst, nrows=None, where=None,
                         includeConfidances=False, dataset_id=None,
                         workspace=None, job_id=None, online=True, hospital='hcgh',
                         max_tsp=None, active_encid=None):
        #--------------------------------------------------------------------------
        # setup for requested features
        #--------------------------------------------------------------------------
        feature_attributes = self.get_feature_attributes(feature_lst, dataset_id)
        data_types = {row['fid']:row['data_type'] for row in feature_attributes}
        s_features = \
            [fa['fid'] for fa in feature_attributes if fa['category'] == 'S']
        twf_features = \
            [fa['fid'] for fa in feature_attributes if fa['category'] == 'TWF']
        t_features = \
            [fa['fid'] for fa in feature_attributes if fa['category'] == 'T']


        if len(t_features)>0:
            raise ValueError('Not yet Implmented for T features')
            # NOTE need to implement query for t category
            # include user specific columns

        if includeConfidances:
            featureConfidances = [fid + '_c' for fid in twf_features]
        else:
            featureConfidances = []
        # user_features = ["user_specific::json->'%(uf)s' as %(uf)s" \
        #     % {'uf':uf[14:]} for uf in feature_lst \
        #         if uf.startswith("user_specific:")]
        # user_feature_names = [uf[14:] for uf in feature_lst \
        #         if uf.startswith("user_specific:")]

        #--------------------------------------------------------------------------
        # build Query
        #--------------------------------------------------------------------------
        columns = ['sub_twf.enc_id', 'sub_twf.tsp'] + twf_features + featureConfidances
        #for f in s_features:
        #    if data_types[f] == 'String':
        #        columns.append("%s.value as %s" % (f, f))
        #    else:
        #        columns.append("cast(%s.value as %s) as %s" % (f, data_types[f], f))
        for f in s_features:
            if data_types[f] == 'String':
                columns.append("%s.value as %s" % (f, f))
            elif f.endswith('_diag') or f.endswith('_hist') or f.endswith('_prob'):
                columns.append("coalesce(cast(%s.value as %s), false) as %s" % (f, data_types[f], f))
            else:
                columns.append("cast(%s.value as %s) as %s" % (f, data_types[f], f))

        feature_name_lst = twf_features + featureConfidances + s_features
        # cdm_twf = '{}.{}_cdm_twf'.format(workspace, job_id) if job_id else 'cdm_twf'
        cdm_twf = 'cdm_twf'
        cdm_s = 'sub_s'
        enc_ids = 'unnest(array[{}]) as enc_id'.format(','.join([str(e) for e in active_encid])) if active_encid else "get_latest_enc_ids('{}') where enc_id in (select enc_id from cdm_s where fid='age' and value::float>=18.0)".format(hospital)
        and_dataset_id = ' and dataset_id = {}'.format(dataset_id) if dataset_id else ''
        and_max_tsp = " and tsp <= '{}'::timestamptz".format(max_tsp) if max_tsp else ''
        sql = '''
        with enc_ids as (
            select enc_id from
            %s
        ),
        sub_twf as (
            SELECT *
            FROM cdm_twf
            where enc_id in (select enc_id from enc_ids)
            %s%s
        ),
        sub_s as (
            select *
            from cdm_s where enc_id in (select enc_id from enc_ids)
            %s
        )
        SELECT %s
        FROM sub_twf
        ''' % (enc_ids,
               and_dataset_id,
               and_max_tsp,
               and_dataset_id,
               ",".join(columns))
        for f in s_features:
            sql += """
            left outer join %(cdm_s)s as %(fid)s
            ON sub_twf.enc_id = %(fid)s.enc_id
            AND %(fid)s.fid = '%(fid)s'
            """% {'fid': f, 'cdm_s': cdm_s}
        sql += " order by enc_id, tsp"

        if nrows and nrows > 0:
            sql += " limit %d" % nrows

        return sql, feature_name_lst

    def query_all_twf_features(self, nrows=0, has_conf=True):
        query_cols_sql = \
        """ select column_name from information_schema.columns
            where table_name = 'cdm_twf'
        """
        if not has_conf:
            query_cols_sql += " and column_name not like '%_c'"
        cursor = self.conn.execute(query_cols_sql)
        cols = cursor.fetchall()
        column_names = [col['column_name'] for col in cols]

        print(query_cols_sql)
        select_cols = ",".join(column_names)
        sql = "SELECT %s FROM cdm_twf order by enc_id, tsp" % select_cols
        if nrows > 0:
            sql += " limit %d" % nrows
        cursor = self.conn.execute(sql)
        twf_data = cursor.fetchall()

        return (column_names, np.asarray(twf_data))

    def enc_id_exist(self, enc_id):
        sql = "SELECT * from pat_enc where enc_id = '%s'" % enc_id
        cursor = self.conn.execute(sql)
        res = cursor.fetchall()

        if len(res) == 1:
            return True
        else:
            return False

    def get_cdm_s(self, enc_id, feature_list):
        """
        return a dictionary with keys in feature_list and their values
        """

        res = {}
        sql = "select value%s from cdm_s where enc_id = %s and fid = '%s'"
        if isinstance(feature_list, str):
            feature_list = [feature_list]
        feature_attributes = self.get_feature_attributes(feature_list)
        data_types = {row['fid']:row['data_type'] for row in feature_attributes}

        for fid in feature_list:
            data_type = data_types[fid]
            if data_type == 'String':
                data_type = ''
            else:
                data_type = '::' + data_type
            try:
                cursor = self.conn.execute(sql % (data_type, enc_id, fid))
                value = cursor.fetchall()[0][0]
                res[fid] = value
            except IndexError:
                if data_type == '::Boolean':
                    res[fid] = False
                else:
                    res[fid] = None
        return res

    def get_cdm_t(self, enc_id, feature_list):
        """
        return a dictionary with keys in feature_list
        and their values with timestamps
        """

        res = {}
        sql = "select tsp, value%s from cdm_t where enc_id = %s and fid = '%s'"
        if isinstance(feature_list, str):
            feature_list = [feature_list]
        feature_attributes = self.get_feature_attributes(feature_list)
        data_types = {row['fid']:row['data_type'] for row in feature_attributes}

        for fid in feature_list:
            data_type = data_types[fid]
            if data_type == 'String':
                data_type = ''
            else:
                data_type = '::' + data_type
            try:
                cursor = self.conn.execute(sql % (data_type, enc_id, fid))
                value = cursor.fetchall()
                res[fid] = value
            except IndexError:
                res[fid] = None
        return res

    def get_cdm_twf(self, enc_id, feature_list):
        """
        return a numpy 2d array
        """
        sql = "select tsp, %s from cdm_twf where enc_id = %s order by tsp"
        if isinstance(feature_list, list):
            feature_list_c = [f + "_c" for f in feature_list]
            feature_list = ",".join(feature_list + feature_list_c)
        else:
            feature_list = feature_list + "," + feature_list + "_c"
        print(feature_list_c)
        print(feature_list)
        cursor = self.conn.execute(sql % (feature_list, enc_id))
        res = cursor.fetchall()

        return res

    def get_cdm_twf_first_true(self, enc_id, feature_list):
        """
        return a dictionary with keys in feature_list
        and their first timestamp when the value is true
        """

        res = {}
        sql = """
        select tsp from cdm_twf
        where enc_id = %s and %s::int = 1
        order by tsp limit 1
        """
        if isinstance(feature_list, str):
            feature_list = [feature_list]
        for fid in feature_list:
            try:
                cursor = self.conn.execute(sql % (enc_id, fid))
                value = cursor.fetchone()
                if value is not None:
                    res[fid] = value[0]
            except IndexError:
                res[fid] = None

        return res

    def get_trewscore_details(self, enc_id):
        """
        return a numpy 2d array
        """
        sql = """
        select * from trewscore_details
        where enc_id = %s order by tsp
        """
        cursor = self.conn.execute(sql % enc_id)
        res = cursor.fetchall()

        return res

    def get_feature_weights(self):
        """
        return a numpy 2d array
        """
        sql = """
        select * from trewscore_feature_weights
        """
        cursor = self.conn.execute(sql)
        res = cursor.fetchall()

        return res

    def get_user_specific(self, enc_id, feature):
        """
        return a numpy 2d array
        """
        sql = """
        select tsp, user_specific::json->'%s'
        from cdm_twf where enc_id = %s order by tsp
        """
        cursor = self.conn.execute(sql % (feature, enc_id))
        res = cursor.fetchall()

        return res

    def insert_report(self, report_string):
        sql = """
        INSERT INTO model_training_report (report, create_at) VALUES ('{}', now());
        """
        self.conn.execute(text(sql.format(report_string)))
