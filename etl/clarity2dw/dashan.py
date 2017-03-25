"""
dashan.py
"""


from create_database import create_database, remove_database, create_cdm_twf
from datalink import DataLink
from resources import CDM, PatEnc
from derive_main import derive_feature, get_derive_seq
from dashan_config import Config
import criteria
import os
import csv
import criteria
from sqlalchemy import create_engine
from StringIO import StringIO
import codecs
from inpatient_updater import load
import datetime
class Dashan (CDM):
    """
    The class of Dashan instance
    """

    def __init__(self, instance_name):
        self.name = instance_name
        self.config = Config(instance_name)
        self.log = self.config.log
        self.db_conn_string = self.config.get_db_conn_string()

    def create(self):
        """
        Create dashan postgres database
        """
        create_database(self.name, config=self.config)
        for datalink_id in self.config.datalinks:
            dl = DataLink(self, datalink_id)
            # Create DataLink in dashan instance database
            dl.create()
        self._create_cdm_twf()

    def create_dbschema(self, schema_folder):
        # create a database schema folder which includes
        # schema generation sql script and required CSV files

        # Make sure the schema_folder exists
        if not os.path.isabs(schema_folder):
            cwd = os.getcwd()
            schema_folder = os.path.join(cwd, schema_folder)
        print "schema_folder:", schema_folder

        if not os.path.isdir(schema_folder):
            os.mkdir(schema_folder)

        # generate static tables, e.g., cdm_t
        schema_sql = ""
        sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), \
        'sql/sql_functions.sql')
        with open(sql_path, 'r') as sql_f:
            schema_sql = sql_f.read()

        sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), \
            'sql/create_dbschema.sql')
        with open(sql_path, 'r') as sql_f:
            schema_sql += sql_f.read()

        sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), \
            'sql/fillin.sql')
        with open(sql_path, 'r') as sql_f:
            schema_sql += sql_f.read()

        # include frontend tables
        schema_sql += criteria.create_online_criteria_sql

        # populate basic definitions, e.g., cdm_feature.csv
        cdm_feature = None
        cdm_feature_csv = self.config.CDM_FEATURE_CSV
        cdm_function_csv = self.config.CDM_FUNCTION_CSV
        function_out_file = os.path.join(schema_folder, 'CDM_Function_%s.csv' % self.name)
        feature_out_file = os.path.join(schema_folder, 'CDM_Feature_%s.csv' % self.name)
        for datalink_id in self.config.datalinks:
            dl = DataLink(self, datalink_id)
            # combine csv and its extension if extension exists
            if dl.config['DATALINK_EX']['function_extension']:
                function_extension_file = os.path.join(dl.datalink_folder,\
                    dl.config['DATALINK_EX']['function_extension'])
                self.combine_csv_extension(cdm_function_csv,
                                                              function_extension_file, function_out_file)
                cdm_function_csv = function_out_file
            if dl.config['DATALINK_EX']['feature_extension']:
                feature_extension_file = os.path.join(dl.datalink_folder,\
                    dl.config['DATALINK_EX']['feature_extension'])
                cdm_feature = self.combine_csv_extension(cdm_feature_csv,
                                                                                        feature_extension_file, feature_out_file)
                cdm_feature_csv = feature_out_file
        # copy csv to the database
        copy_sql = "\copy %s from '%s' with csv header delimiter as ',';\n"
        schema_sql += copy_sql % ('cdm_function', function_out_file)
        schema_sql += copy_sql % ('cdm_feature', feature_out_file)

        # create dynamic tables, e.g., cdm_twf
        sql_create_cdm_twf = \
        """DROP TABLE IF EXISTS cdm_twf;
            CREATE TABLE cdm_twf (
                enc_id  integer REFERENCES pat_enc(enc_id),
                tsp     timestamptz,
                %s
                meta_data json,
                PRIMARY KEY (enc_id, tsp)
                );
        """
        sql_columns = ""
        for fid in cdm_feature:
            row = cdm_feature[fid]
            data_type = row[2]
            if row[1] == 'TWF':
                sql_columns += "%s %s," % (fid, data_type)
                sql_columns += "%s_c integer," % fid
        schema_sql += sql_create_cdm_twf % sql_columns
        schema_sql += """
        DROP SCHEMA IF EXISTS workspace;
        CREATE SCHEMA workspace;"""
        schema_file = os.path.join(schema_folder, 'create_dbschema.sql')
        with open(schema_file, 'w') as out:
            out.write(schema_sql)
        # TODO: generate delete_dbschema.sql
        # delete_schema_file = os.path.join(schema_folder, 'delete_dbschema.sql')
        # delete_schema_sql = """

        # """
        # with open()


    def combine_csv_extension(self, csv_file, csv_extension_file, out_file):
        # read the csv_file and csv_extension_file
        # combine them and write to out_file
        field_names = None
        # by default, the first row is the key
        row_dict = {}
        with open(csv_file, 'rU') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                if not field_names:
                    field_names = row
                else:
                    key = row[0]
                    row_dict[key] = row
        with open(csv_extension_file, 'rU') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            ignored_header = False
            for row in reader:
                    if ignored_header:
                        key = row[0]
                        # override the row in the row_dict
                        row_dict[key] = row
                    else:
                        ignored_header = True
        # write the merged row_dict to the output csv file
        with open(out_file, 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(field_names)
            for key in row_dict:
                row = row_dict[key]
                writer.writerow(row)
        return row_dict


    def delete(self):
        """
        delete dashan postgres database
        """
        remove_database(self.name, config=self.config)

    def _create_cdm_twf(self):
        """
        Create cdm_twf table based on all features in cdm_feature table
        """
        create_cdm_twf(dashan=self)

    def call_datalink(self, datalink_id, cmd, args=None):
        """
        call the datalink with datalink_id and run the command,
        e.g., 'bedded_patients get'
        """
        if args:
            self.log.info("call datalink %s and run command: %s %s" \
                % (datalink_id, cmd, args))
        else:
            self.log.info("call datalink %s and run command: %s" \
                % (datalink_id, cmd))

        dl = DataLink(self, datalink_id)
        dl.call(cmd, args=args)

    def calculate_online_trewscore(self, datalink_id, results):
        dl = DataLink(self, datalink_id)
        self.connect()
        dl.calculate_online_trewscore(results)
        self.disconnect()

    def calculate_online_criteria(self, job_id):
        criteria.calculate(self, job_id)


    def get_datalink(self, datalink_id):
        return DataLink(self, datalink_id)

    def get_visit_id_to_enc_id_mapping(self):
        mapping = {}
        pat_enc = PatEnc(self.config)
        pat_enc.connect()
        # all select methods return a server-side cursor
        server_cursor = pat_enc.select_pat_enc()
        for row in server_cursor:
            mapping[row['visit_id']] = row['enc_id']
        server_cursor.close()
        pat_enc.disconnect()
        return mapping

    def get_pat_id_to_enc_id_mapping(self):
        mapping = {}
        pat_enc = PatEnc(self.config)
        pat_enc.connect()
        # all select methods return a server-side cursor
        server_cursor = pat_enc.select_pat_enc()
        for row in server_cursor:
            if row['pat_id'] in mapping:
                mapping[row['pat_id']].append(row['enc_id'])
            else:
                mapping[row['pat_id']] = [row['enc_id']]
        server_cursor.close()
        pat_enc.disconnect()
        return mapping

    def etl(self, datalink_id):
        self.connect()
        cmd = "etl"
        self.call_datalink(datalink_id, cmd)
        self.disconnect()

    def cdm_data_clean(self):
        self.connect()
        CDM.data_clean(self)
        self.disconnect()

    def fillin(self, cdm_twf_features, recalculate_popmean=True,
               target='main'):
        """
        fillin all measured TWF features
        """
        # XXX: may be we don't have to connect for each feature
        # instead using one connection to fill all features
        self.log.info("fillin start")
        for fid_twf in cdm_twf_features:
            if target == 'main':
                CDM.fillin(self, fid_twf, recalculate_popmean)
            else:
                CDM.fillin(self, fid_twf, recalculate_popmean, table=target+"_cdm_twf")
        self.log.info("fillin completed")

    def derive(self, derive_features, cdm_feature_dict, target='main'):
        """
        derive features sequencially
        """
        self.log.info("derive start")
        # get derive order based on the input derive_features
        derive_feature_list = [cdm_feature_dict[fid] for fid in derive_features]
        derive_feature_order = get_derive_seq(derive_feature_list)
        # derive the features sequentially

        for fid in derive_feature_order:
            self.log.info("deriving fid %s" % fid)
            if cdm_feature_dict[fid]['category'] == 'TWF' and target != 'main':
                derive_feature(cdm_feature_dict[fid], self, twf_table=target+"_cdm_twf")
            else:
                derive_feature(cdm_feature_dict[fid], self)
        self.log.info("derive completed")

    def predict(self, feature_weights, max_score, min_score, cdm_feature_dict, target='main'):
        """
        calculate trewscore and save in trewscore table in cdm_twf
        # for fid in twf_features if cdm_feature_dict[fid]['data_type'] != 'Boolean'  and (fid != 'minutes_since_any_antibiotics' and fid != 'minutes_since_any_organ_fail')] + \
            # ['case when %s::numeric == -60000 then 0 else %s::numeric' % (fid, feature_weights[fid]) \
            # for fid in twf_features if fid == 'minutes_since_any_organ_fail' or fid == 'minutes_since_any_antibiotics']  + \
           # [fid for fid in twf_features if cdm_feature_dict[fid]['data_type'] != 'Boolean'   and (fid != 'minutes_since_any_antibiotics' and fid != 'minutes_since_any_organ_fail')] + \
            # [fid for fid in twf_features if fid == 'minutes_since_any_organ_fail' or fid == 'minutes_since_any_antibiotics'] + \

        """
        self.log.info("predict start")
        num_feature = len(feature_weights)
        twf_features = [fid for fid in feature_weights \
            if cdm_feature_dict[fid]['category'] == 'TWF']
        s_features = [fid for fid in feature_weights \
            if cdm_feature_dict[fid]['category'] == 'S']
        twf_features_times_weights = ['coalesce(((%s::numeric - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\')) / (select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid, fid, fid, feature_weights[fid]) \
            for fid in twf_features if cdm_feature_dict[fid]['data_type'] != 'Boolean' ] + \
             ['coalesce(((%s::int - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\') ) / ( select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid,fid, fid, feature_weights[fid]) \
            for fid in twf_features if cdm_feature_dict[fid]['data_type'] == 'Boolean']
        s_features_times_weights = ['coalesce((( coalesce(%s.value::numeric,0) - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\') ) / (select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid,fid,fid, feature_weights[fid]) \
            for fid in s_features if cdm_feature_dict[fid]['data_type'] != 'Boolean'] + \
            ['coalesce(((  coalesce(%s.value::bool::int,0) - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\') ) / (select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid, fid, fid, feature_weights[fid]) \
            for fid in s_features if cdm_feature_dict[fid]['data_type'] == 'Boolean']
        feature_weight_colnames = \
            [fid for fid in twf_features if cdm_feature_dict[fid]['data_type'] != 'Boolean' ] + \
            [fid for fid in twf_features if cdm_feature_dict[fid]['data_type'] == 'Boolean'] + \
            [fid for fid in s_features if cdm_feature_dict[fid]['data_type'] != 'Boolean'] + \
            [fid for fid in s_features if cdm_feature_dict[fid]['data_type'] == 'Boolean']
        feature_weight_values = [ "(%s - %s)/%s" % (val, min_score/num_feature, max_score - min_score)\
            for val in (twf_features_times_weights + s_features_times_weights)]
        select_clause = ",".join(["%s %s" % (v,k) \
            for k,v in zip(feature_weight_colnames, feature_weight_values)])
        #feature_sum = '+'.join(twf_features_times_weights + s_features_times_weights)
        # select feature values
        # calculate the trewscores
        # TODO maybe need to create index as well
        weight_sum = "+".join(['coalesce(%s,0)'  % col for col in feature_weight_colnames])
        if target == 'main':
            table = 'trews'
            twf_table = 'cdm_twf'
        else:
            table = target + '_trews'
            twf_table = target + '_cdm_twf'
        sql = \
        '''
        drop table if exists %(table)s;
        CREATE TABLE %(table)s AS
        select %(twf_table)s.enc_id, tsp, %(cols)s, null::numeric trewscore
        from %(twf_table)s
        ''' % {'cols':select_clause, 'table': table, 'twf_table': twf_table}
        for f in s_features:
            sql += """ left outer join cdm_s %(fid)s
                      ON %(twf_table)s.enc_id = %(fid)s.enc_id
                      AND %(fid)s.fid = '%(fid)s'
                   """ % {'fid': f, 'twf_table': twf_table}
        update_sql = \
        """
        ;update %(table)s set trewscore = %(sum)s;
        """ % {'sum':weight_sum, 'table':table, 'twf_table': twf_table}
        sql += update_sql
        self.log.info("predict trewscore:" + sql)
        self.query_with_sql(sql)
        self.log.info("predict completed")

    def to_sql(self, job_id, table_type, df, dtypes=None, schema='public', if_exists='replace'):
        nrows = df.shape[0]
        table_name = '%s_%s' % (job_id, table_type)
        self.log.info("saving data frame to %s: nrows = %s" % (table_name, nrows))
        engine = create_engine(self.config.get_db_conn_string_sqlalchemy())
        if dtypes:
            df = df.astype(dtypes)
            self.log.info("%s: dashan.to_sql: converted dict to string %s" \
                % (job_id, table_type))
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, schema=schema)
        self.log.info(table_name + " saved: nrows = %s" % nrows)
        if nrows > 0 and not df.empty:
            self.log.info(table_name + " saved: nrows = %s" % nrows)
        else:
            self.log.warn("zero rows in the data frame:%s" % table_name)

    def copy_to(self, job_id, table_type, df, dtypes=None, schema='public', append=False):
        nrows = df.shape[0]
        table_name = 'workspace.%s_%s' % (job_id, table_type)
        self.log.info("saving data frame to %s: nrows = %s" % (table_name, nrows))
        if dtypes:
            df = df.astype(dtypes)
            self.log.info("%s: dashan.to_sql: converted dict to string %s" \
                % (job_id, table_type))
        conn = self.connect(secondary=True)
        # df.to_sql(table_name, engine, if_exists='replace', index=False, schema=schema)
        buf = StringIO()
        # saving a data frame to a buffer (same as with a regular file):
        df.to_csv(buf, index=False, sep='\t', header=False, \
            quoting=csv.QUOTE_NONE, date_format="ISO", escapechar=" ")
        cols = ",".join([ "%s text" % col for col in df.columns.values.tolist()])
        # buf = codecs.EncodedFile(buf,"LATIN1", "UTF8")
        cur = conn.cursor()
        buf.seek(0)
        prepare_table = \
        """DROP table if exists %(tab)s;
        create table %(tab)s (
            %(cols)s
        );
        """ % {'tab': table_name, 'cols': cols}
        self.log.info("create table: " + prepare_table)
        cur.execute(prepare_table)

        if nrows > 0 and not df.empty:
            cur.copy_from(buf, table_name)
            self.log.info(table_name + " saved: nrows = %s" % nrows)
        else:
            self.log.warn("zero rows in the data frame:%s" % table_name)
        conn.commit()
        conn.close()




    def update_notifications(self):
        self.log.info("updating notifications")
        self.query_with_sql("select update_notifications()")
        self.push_notifications_to_epic()
        self.log.info("updated notifications")


    def push_notifications_to_epic(self, server_name='stage'):
        self.log.info("pushing notifications to epic %s" % server_name)
        cursor = self.select_with_sql("""
            select * from get_notifications_for_epic(null)
            """)
        notifications = cursor.fetchall()
        patients = [ {'pat_id': n['pat_id'], 'visit_id': n['visit_id'], 'notifications': n['count'],
                            'current_time': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")} for n in notifications]
        cursor.close()
        client_id = os.environ['jhapi_client_id'],
        client_secret = os.environ['jhapi_client_secret']
        loader = load.Loader(server_name, client_id, client_secret)
        loader.load_notifications(patients)
        self.log.info("pushed notifications to epic %s" % server_name)

    def load_to_cdm(self, job_id, datalink_id):
        dl = DataLink(self, datalink_id)
        # load transformed features from this @job_id to @job_id_cdm_twf
        # insert new bedded_patients
        # for new patients insert into pat_enc
        insert_new_patients_sql = """
        insert into pat_enc (pat_id, visit_id)
        select bp.pat_id, bp.visit_id
        from workspace.%s_bedded_patients_transformed bp
        left join pat_enc pe on bp.visit_id = pe.visit_id
        where pe.enc_id is null;
        """ % job_id
        self.log.info("%s: insert new bedded_patients: %s" % (job_id, insert_new_patients_sql))
        self.query_with_sql(insert_new_patients_sql)
        #self.query_with_sql("select * from cdm_t;select * from %s_flowsheets_transformed" % job_id)


        # create snapshot cdm_twf tables for this job, i.e., all bedded patients
        create_job_cdm_twf_table = """
        DROP TABLE IF EXISTS workspace.%(job)s_cdm_twf CASCADE;
        create table workspace.%(job)s_cdm_twf as
        select cdm_twf.* from cdm_twf
        inner join pat_enc on pat_enc.enc_id = cdm_twf.enc_id
        inner join workspace.%(job)s_bedded_patients_transformed bp
            on bp.visit_id = pat_enc.visit_id;
        alter table workspace.%(job)s_cdm_twf add primary key (enc_id, tsp);
        """ % {'job':job_id}
        self.log.info("%s: create job cdm_twf table: %s" % (job_id, create_job_cdm_twf_table))
        self.query_with_sql(create_job_cdm_twf_table)

        # upsert new measurement from @job_id
        import_raw_features = """
        -- age
        INSERT INTO cdm_s (enc_id, fid, value, confidence)
        select pe.enc_id, 'age', bp.age, 1 as c from workspace.%(job)s_bedded_patients_transformed bp
            inner join pat_enc pe on pe.visit_id = bp.visit_id
        ON CONFLICT (enc_id, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        self.query_with_sql(import_raw_features)
        import_raw_features = """
        -- gender
        INSERT INTO cdm_s (enc_id, fid, value, confidence)
        select pe.enc_id, 'gender', bp.gender, 1 as c from workspace.%(job)s_bedded_patients_transformed bp
            inner join pat_enc pe on pe.visit_id = bp.visit_id
        ON CONFLICT (enc_id, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        self.query_with_sql(import_raw_features)
        import_raw_features = """
        -- diagnosis
        INSERT INTO cdm_s (enc_id, fid, value, confidence)
        select pe.enc_id, json_object_keys(diagnosis::json), 'True', 1
        from workspace.%(job)s_bedded_patients_transformed bp
            inner join pat_enc pe on pe.visit_id = bp.visit_id
        ON CONFLICT (enc_id, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        self.query_with_sql(import_raw_features)
        import_raw_features = """
        -- problem
        INSERT INTO cdm_s (enc_id, fid, value, confidence)
        select pe.enc_id, json_object_keys(problem::json), 'True', 1
        from workspace.%(job)s_bedded_patients_transformed bp
            inner join pat_enc pe on pe.visit_id = bp.visit_id
        ON CONFLICT (enc_id, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        self.query_with_sql(import_raw_features)
        import_raw_features = """
        -- history
        INSERT INTO cdm_s (enc_id, fid, value, confidence)
        select pe.enc_id, json_object_keys(history::json), 'True', 1
        from workspace.%(job)s_bedded_patients_transformed bp
            inner join pat_enc pe on pe.visit_id = bp.visit_id
        ON CONFLICT (enc_id, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        self.query_with_sql(import_raw_features)
        import_raw_features = """
        -- hospital
        INSERT INTO cdm_s (enc_id, fid, value, confidence)
        select pe.enc_id, 'hospital', hospital, 1
        from workspace.%(job)s_bedded_patients_transformed bp
            inner join pat_enc pe on pe.visit_id = bp.visit_id
        ON CONFLICT (enc_id, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        self.query_with_sql(import_raw_features)
        import_raw_features = """
        -- admittime
        INSERT INTO cdm_s (enc_id, fid, value, confidence)
        select pe.enc_id, 'admittime', admittime, 1
        from workspace.%(job)s_bedded_patients_transformed bp
            inner join pat_enc pe on pe.visit_id = bp.visit_id
        ON CONFLICT (enc_id, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        self.query_with_sql(import_raw_features)

        '''
        flowsheets -> cdm_t
        '''
        import_raw_features = \
        """INSERT INTO cdm_t (enc_id, tsp, fid, value, confidence)
                select pat_enc.enc_id, fs.tsp::timestamptz, fs.fid, last(fs.value), 0 from workspace.%(job)s_flowsheets_transformed fs
                    inner join pat_enc on pat_enc.visit_id = fs.visit_id
                    inner join cdm_feature on fs.fid = cdm_feature.fid and cdm_feature.category = 'T'
                where fs.tsp <> 'NaT' and fs.tsp::timestamptz < now()
                group by pat_enc.enc_id, tsp, fs.fid
            ON CONFLICT (enc_id, tsp, fid)
                DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        try:
            self.query_with_sql(import_raw_features)
        except Exception as e:
            self.log.error(e)
        '''
        lab_results -> cdm_t
        '''
        import_raw_features = \
        """INSERT INTO cdm_t (enc_id, tsp, fid, value, confidence)
            select pat_enc.enc_id, lr.tsp::timestamptz, lr.fid, lr.value, 0 from workspace.%(job)s_lab_results_transformed lr
                inner join pat_enc on pat_enc.visit_id = lr.visit_id
                inner join cdm_feature on lr.fid = cdm_feature.fid and cdm_feature.category = 'T'
            where lr.tsp <> 'NaT' and lr.tsp::timestamptz < now()
        ON CONFLICT (enc_id, tsp, fid)
            DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        self.query_with_sql(import_raw_features)

        '''
        adt (location_history_transformed) -> cdm_t
        '''
        import_raw_features = \
        """INSERT INTO cdm_t (enc_id, tsp, fid, value, confidence)
            select pat_enc.enc_id, lr.tsp::timestamptz, lr.fid, lr.value, 0 from workspace.%(job)s_location_history_transformed lr
                inner join pat_enc on pat_enc.visit_id = lr.visit_id
                inner join cdm_feature on lr.fid = cdm_feature.fid and cdm_feature.category = 'T'
            where lr.tsp <> 'NaT' and lr.tsp::timestamptz < now()
            ON CONFLICT (enc_id, tsp, fid)
               DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        self.query_with_sql(import_raw_features)

        '''
        MAR -> cdm_t
        '''
        import_raw_features = \
        """INSERT INTO cdm_t (enc_id, tsp, fid, value, confidence)
            select pat_enc.enc_id, mar.tsp::timestamptz, mar.fid,
                json_build_object('dose',SUM(mar.dose_value::numeric),'action',last(mar.action))
                , 0
            from workspace.%(job)s_med_admin_transformed mar
                inner join pat_enc on pat_enc.visit_id = mar.visit_id
                inner join cdm_feature on cdm_feature.fid = mar.fid
            where isnumeric(mar.dose_value) and mar.tsp <> 'NaT' and mar.tsp::timestamptz < now() and mar.fid ~ 'dose'
            group by pat_enc.enc_id, tsp, mar.fid
        ON CONFLICT (enc_id, tsp, fid)
            DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
        -- fluids and others
        INSERT INTO cdm_t (enc_id, tsp, fid, value, confidence)
            select pat_enc.enc_id, mar.tsp::timestamptz, mar.fid,
                max(mar.dose_value::numeric), 0
            from workspace.%(job)s_med_admin_transformed mar
                inner join pat_enc on pat_enc.visit_id = mar.visit_id
                inner join cdm_feature on cdm_feature.fid = mar.fid
            where isnumeric(mar.dose_value) and mar.tsp <> 'NaT' and mar.tsp::timestamptz < now() and mar.fid not ilike 'dose'
            group by pat_enc.enc_id, tsp, mar.fid
        ON CONFLICT (enc_id, tsp, fid)
            DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
        """ % {'job': job_id}
        self.log.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
        self.query_with_sql(import_raw_features)

        '''
        flowsheets -> cdm_twf
        '''
        select_twf_features = """
        select distinct c.fid, c.data_type from cdm_feature c
        inner join workspace.%(job)s_%(tab)s_transformed fs on fs.fid = c.fid
        where c.category = 'TWF'
        """
        cursor = self.select_with_sql(select_twf_features % {'job': job_id, 'tab': 'flowsheets'})
        twf_features = cursor.fetchall()
        cursor.close()
        upsert_twf_features = """
        insert into workspace.%(job)s_cdm_twf (enc_id, tsp, %(fid)s, %(fid)s_c)
            select pat_enc.enc_id, raw.tsp::timestamptz, last(raw.value::%(dt)s), 0
            from workspace.%(job)s_%(tab)s_transformed raw
            inner join pat_enc on pat_enc.visit_id = raw.visit_id
            where raw.fid = '%(fid)s' and isnumeric(raw.value) and raw.tsp <> 'NaT' and raw.tsp::timestamptz < now()
            group by pat_enc.enc_id, tsp
        ON CONFLICT (enc_id, tsp)
            DO UPDATE SET %(fid)s = EXCLUDED.%(fid)s, %(fid)s_c = EXCLUDED.%(fid)s_c;
        """
        for feature in twf_features:
            update_twf_feature = upsert_twf_features % {'fid': feature['fid'], 'dt': feature['data_type'],
                                                                                      'job': job_id, 'tab': 'flowsheets'}
            self.log.info("%s: update_twf_feature: %s" % (job_id, update_twf_feature))
            try:
                self.query_with_sql(update_twf_feature)
            except Exception as e:
                self.log.error(e)
                self.connect()

        '''
        lab_results -> cdm_twf
        '''
        select_twf_features = """
        select distinct c.fid, c.data_type from cdm_feature c
        inner join workspace.%(job)s_%(tab)s_transformed fs on fs.fid = c.fid
        where c.category = 'TWF'
        """
        cursor = self.select_with_sql(select_twf_features % {'job': job_id, 'tab': 'lab_results'})
        twf_features = cursor.fetchall()
        cursor.close()
        upsert_twf_features = """
        insert into workspace.%(job)s_cdm_twf (enc_id, tsp, %(fid)s, %(fid)s_c)
            select pat_enc.enc_id, raw.tsp::timestamptz, last(raw.value::%(dt)s), 0
            from workspace.%(job)s_%(tab)s_transformed raw
            inner join pat_enc on pat_enc.visit_id = raw.visit_id
            where raw.fid = '%(fid)s' and isnumeric(raw.value) and raw.tsp <> 'NaT' and raw.tsp::timestamptz < now()
            group by pat_enc.enc_id, tsp
        ON CONFLICT (enc_id, tsp)
            DO UPDATE SET %(fid)s = EXCLUDED.%(fid)s, %(fid)s_c = EXCLUDED.%(fid)s_c;
        """
        for feature in twf_features:
            update_twf_feature = upsert_twf_features % {'fid': feature['fid'], 'dt': feature['data_type'],
                                                                                      'job': job_id, 'tab': 'lab_results'}
            self.log.info("%s: update_twf_feature: %s" % (job_id, update_twf_feature))
            try:
                self.query_with_sql(update_twf_feature)
            except Exception as e:
                self.log.error(e)
                self.connect()
        self.log.info("load raw data to cdm completed")

        '''
        MAR -> cdm_twf
        Note{zad}: usually no twf features in MAR but for safty we cover them in MAR
        '''
        select_twf_features = """
        select distinct c.fid, c.data_type from cdm_feature c
        inner join workspace.%(job)s_%(tab)s_transformed fs on fs.fid = c.fid
        where c.category = 'TWF'
        """
        cursor = self.select_with_sql(select_twf_features % {'job': job_id, 'tab': 'med_admin'})
        twf_features = cursor.fetchall()
        cursor.close()
        upsert_twf_features = """
        insert into workspace.%(job)s_cdm_twf (enc_id, tsp, %(fid)s, %(fid)s_c)
            select pat_enc.enc_id, raw.tsp::timestamptz, last(raw.dose_value::%(dt)s), 0
            from workspace.%(job)s_%(tab)s_transformed raw
            inner join pat_enc on pat_enc.visit_id = raw.visit_id
            where raw.fid = '%(fid)s' and isnumeric(raw.dose_value) and raw.tsp <> 'NaT' and raw.tsp::timestamptz < now()
            group by pat_enc.enc_id, tsp
        ON CONFLICT (enc_id, tsp)
            DO UPDATE SET %(fid)s = EXCLUDED.%(fid)s, %(fid)s_c = EXCLUDED.%(fid)s_c;
        """
        for feature in twf_features:
            update_twf_feature = upsert_twf_features % {'fid': feature['fid'], 'dt': feature['data_type'],
                                                                                      'job': job_id, 'tab': 'med_admin'}
            self.log.info("%s: update_twf_feature: %s" % (job_id, update_twf_feature))
            try:
                self.query_with_sql(update_twf_feature)
            except Exception as e:
                self.log.error(e)
                self.connect()
        self.log.info("load raw data to cdm completed")

    def calculate_trewscore(self, job_id, datalink_id):
        dl = DataLink(self, datalink_id)
        dl.calculate_trewscore(job_id)

    def submit_results(self, job_id):
        # submit to cdm_twf
        # submit to trews
        self.log.info("submit start")
        self.log.info("%s: submitting results ..." % job_id)
        select_all_colnames = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '%(table)s';
        """

        submit_twf = """
        insert into cdm_twf
        (
            select * from workspace.%(job)s_cdm_twf
            )
        on conflict (enc_id, tsp)
            do update set %(set_columns)s;
        SELECT drop_tables('workspace', '%(job)s_cdm_twf');
        """
        submit_trews = """
        create table if not exists trews (like workspace.%(job)s_trews,
            unique (enc_id, tsp)
            );
        insert into trews (enc_id, tsp, %(columns)s) (
            select enc_id, tsp, %(columns)s from workspace.%(job)s_trews
            )
        on conflict (enc_id, tsp)
            do update set %(set_columns)s;
        SELECT drop_tables('workspace', '%(job)s_trews');
        """
        cursor = self.select_with_sql(select_all_colnames % {'table': 'cdm_twf'})
        colnames = [row[0] for row in cursor.fetchall() if row[0] != 'enc_id' and row[0] != 'tsp']
        cursor.close()
        twf_set_columns = ",".join([
            "%(col)s = excluded.%(col)s" % {'col': colname} for colname in colnames
        ])
        self.log.info(submit_twf % {'job': job_id, 'set_columns': twf_set_columns} )
        self.query_with_sql(submit_twf % {'job': job_id, 'set_columns': twf_set_columns} )
        cursor = self.select_with_sql(select_all_colnames \
            % {'table': '%s_trews' % job_id})
        colnames = [row[0] for row in cursor.fetchall() if row[0] != 'enc_id' and row[0] != 'tsp']
        cursor.close()
        trews_set_columns = ",".join([
            "%(col)s = excluded.%(col)s" % {'col': colname} for colname in colnames
        ])
        trews_columns = ",".join(colnames)

        self.log.info(submit_trews % {'job': job_id, 'set_columns': trews_set_columns, 'columns': trews_columns} )
        self.query_with_sql(submit_trews % {'job': job_id, 'set_columns': trews_set_columns, 'columns': trews_columns} )
        self.log.info("%s: results submitted" % job_id)
        self.log.info("submit completed")

    def drop_tables(self, job_id, days_offset=2):
        day = (datetime.datetime.now() - datetime.timedelta(days=days_offset)).strftime('%m%d')
        self.log.info("cleaning data in workspace for day:%s" % day)
        self.query_with_sql("select drop_tables_pattern('workspace', '%%_%s');" % day)
        self.log.info("cleaned data in workspace for day:%s" % day)