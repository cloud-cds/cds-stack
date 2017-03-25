import sys, os, csv, subprocess
from dashan import Dashan

# Populate the data sequentially
def populate_data(dashan_id, datalink_id, fid):
    sys.path.append('../../src/ews_server/')
    from populate_cdm import populate_cdm
    print fid
    populate_cdm(dashan_id, datalink_id, fid_input=fid, nproc=1, debug=True)

# Format the values for the database insertion
def frmt(string, no_quotes, last):
    return_string = string
    if string == '':
        return_string = "null"
    else:
        # replace single quote to two single quotes when it is in the middle
        if not no_quotes:
            return_string = string.replace("'","''")
            return_string = "'" + return_string + "'"
    if not last:
        return_string = return_string + ","
    return return_string


# Main
if __name__ == '__main__':
    # Take the feature id as the input
    try:
        if len(sys.argv) == 3:
            dashan_id = sys.argv[1]
            datalink_id = dashan_id
            fid = sys.argv[2]
        elif len(sys.argv) == 4:
            dashan_id = sys.argv[1]
            datalink_id = sys.argv[2]
            fid = sys.argv[3]
        dashan_instance = Dashan(dashan_id)
    except Exception as e:
        print "Error: Needs fid as the only input argument"
        quit(0)
    cdm_feature = None
    # Save the cdm_feature csv file from github as a reader object
    with open('../../conf/CDM_Feature.csv', 'rU') as csvfile:
        cdm_feature_reader = csv.reader(csvfile)
        # Save the rows with the correct feature from /conf/CDM_Feature.csv
        for row in cdm_feature_reader:
            if row[0] == fid:
                cdm_feature = row
    # replace the cdm_feature if the extension exists
    with open('../../conf/datalink/%s/feature_extension.csv' % datalink_id, 'rU') as csvfile:
        cdm_feature_reader = csv.reader(csvfile)
        # Save the rows with the correct feature from /conf/CDM_Feature.csv
        for row in cdm_feature_reader:
            if row[0] == fid:
                cdm_feature = row
    print "current feature:", cdm_feature
    # Delete existing entries in the feature_mapping database
    delete_entry_sql = ""
    if cdm_feature[1] == "S":
        delete_entry_sql = "delete from cdm_s where fid = '%s';" % fid
    elif cdm_feature[1] == "T":
        delete_entry_sql = "delete from cdm_t where fid = '%s';" % fid
    if cdm_feature is None:
        print "Error: no %s in cdm_feature.csv" % fid
        quit(0)
    if cdm_feature[3] == 'yes':
        # measured feature
        print "This is measured feature"
        # Save the feature_mapping csv file from github as a reader object
        with open('../../conf/datalink/%s/feature_mapping.csv' % datalink_id,\
         'rU') as csvfile:
            feature_mapping_reader = csv.reader(csvfile)
            # Save the rows with the correct feature from /conf/dblink/hcgh_1608/feature_mapping.csv
            # Note: There could be more than 1
            feature_mapping = []
            for row in feature_mapping_reader:
                if row[0] == fid:
                    # Print out both and ask which is correct
                    print "find feature_mapping:", row
                    feature_mapping.append(row)

        # Check to see if there is more than one feature_mapping
        try:
            print feature_mapping[1][0]
        except IndexError as e:
            pass
        else:
            print "Error: more than one feature_mapping"
            quit(0)




        # Delete existing feature definition in the cdm_feature table
        delete_sql = """
        delete from datalink_feature_mapping where fid = '%(fid)s';
        delete from cdm_feature where fid = '%(fid)s';
        """ % {'fid':fid}

        delete_sql = delete_entry_sql + delete_sql
        # Check to see if cdm_feature has a value transform_func_id
        transform_func_id = feature_mapping[0][-1]
        if transform_func_id:
            cdm_function = None
            with open('../../conf/CDM_Function.csv', 'rU') as csvfile:
                cdm_function_reader = csv.reader(csvfile)
                # Save the rows with the correct feature from /conf/CDM_Feature.csv
                for row in cdm_function_reader:
                    if row[0] == transform_func_id:
                        cdm_function = row
            with open('../../conf/datalink/%s/function_extension.csv' \
                % datalink_id, 'rU') as csvfile:
                cdm_function_reader = csv.reader(csvfile)
                # Save the rows with the correct feature from /conf/CDM_Feature.csv
                for row in cdm_function_reader:
                    if row[0] == transform_func_id:
                        cdm_function = row
            if not cdm_function:
                print "the current function %s is not included in CDM_Function.csv" \
                % transform_func_id
                quit(0)
            else:
                print "the current function:", cdm_function
                delete_func_sql = "delete from cdm_function where func_id = '%s';" % transform_func_id
                delete_sql += delete_func_sql
            # check whether the function needs to be added
            print "Make sure you write a transform function for this feature"
            insert_func_sql = """
            insert into cdm_function values ('%(func_id)s', '%(type)s', '%(desc)s');
            """ % {'func_id': transform_func_id,
                   'type': cdm_function[1],
                   'desc': cdm_function[2].replace("'","''")}
            delete_sql += insert_func_sql

        # Create the query for updating the CDM_Feature database
        query_cdm_feature = delete_sql + "insert into cdm_feature values (" + \
            frmt(cdm_feature[0], 0, 0) + frmt(cdm_feature[1], 0, 0) + \
            frmt(cdm_feature[2], 0, 0) + frmt(cdm_feature[3], 0, 0) + \
            frmt(cdm_feature[4], 0, 0) + frmt(cdm_feature[5], 0, 0) + \
            frmt(cdm_feature[6], 1, 0) + frmt(cdm_feature[7], 0, 0) + \
            frmt(cdm_feature[8], 0, 0) + frmt(cdm_feature[9], 0, 0) + \
            frmt(cdm_feature[10], 0, 0) + frmt(cdm_feature[11], 0, 1) + ");"


        # Create the strings for updating the feature_mapping database
        query_feature_mapping = "insert into datalink_feature_mapping values (" + \
            frmt(feature_mapping[0][0], 0, 0) + frmt(feature_mapping[0][1], 0, 0) + \
            frmt(feature_mapping[0][2], 0, 0) + ("'%s', " % datalink_id) + \
            frmt(feature_mapping[0][3], 0, 0) + frmt(feature_mapping[0][4], 0, 0) + \
            frmt(feature_mapping[0][5], 0, 0) + frmt(feature_mapping[0][6], 0, 1) + ");"

        # Create the bash function to do that
        sql_string = query_cdm_feature + "\n" + query_feature_mapping

        # if the second value in the CDM_feature row is 'TWF' ---> it means that it should be in the cdm_twf database
        if cdm_feature[1] == "TWF":
            # Add the fid
            sql_string = sql_string + "\n" + "alter table cdm_twf add column " + \
            str(cdm_feature[0]) + " " + str(cdm_feature[2]).lower() + ";"
            # Add the fid_c
            sql_string = sql_string + "\n" + "alter table cdm_twf add column " + \
            str(cdm_feature[0]) + "_c int;"

        # with open('add_feature.sql', 'wb') as f:
        #     f.write(sql_string)
        # bash_string = "psql -d %s -a -f add_feature.sql" % datalink_id
        # return_code = os.system(bash_string)
        dashan_instance.connect()
        print "run sql:", sql_string
        dashan_instance.query_with_sql(sql_string)
        dashan_instance.disconnect()

        # Start populating the data
        answer = raw_input("Do you want to start populating the data to %s (y/n): " % datalink_id)
        if answer != 'y':
            quit(0)
        else:
            populate_data(dashan_id, datalink_id, fid)
    else:
        print "This is a derive feature."
        delete_sql = """
        delete from cdm_feature where fid = '%(fid)s';
        """ % {'fid':fid}
        delete_sql = delete_entry_sql + delete_sql
        if cdm_feature[7]:
            cdm_function = None
            with open('../../conf/CDM_Function.csv', 'rU') as csvfile:
                cdm_function_reader = csv.reader(csvfile)
                # Save the rows with the correct feature from /conf/CDM_Feature.csv
                for row in cdm_function_reader:
                    if row[0] == cdm_feature[7]:
                        cdm_function = row
            if not cdm_function:
                print "the current function %s is not included in CDM_Function.csv" \
                % cdm_feature[7]
                quit(0)
            # check whether the function needs to be added
            print "Make sure you write a derive function for this feature"
            insert_func_sql = """
            insert into cdm_function values ('%(func_id)s', '%(type)s', '%(desc)s');
            """ % {'func_id': cdm_feature[7],
                   'type': cdm_function[1],
                   'desc': cdm_function[2].replace("'","''")}
            delete_sql += insert_func_sql
            # Create the query for updating the CDM_Feature database
            query_cdm_feature = delete_sql + "insert into cdm_feature values (" + \
            frmt(cdm_feature[0], 0, 0) + frmt(cdm_feature[1], 0, 0) + \
            frmt(cdm_feature[2], 0, 0) + frmt(cdm_feature[3], 0, 0) + \
            frmt(cdm_feature[4], 0, 0) + frmt(cdm_feature[5], 0, 0) + \
            frmt(cdm_feature[6], 1, 0) + frmt(cdm_feature[7], 0, 0) + \
            frmt(cdm_feature[8], 0, 0) + frmt(cdm_feature[9], 0, 0) + \
            frmt(cdm_feature[10], 0, 0) + frmt(cdm_feature[11], 0, 1) + ");"
            # Create the bash function to do that
            sql_string = query_cdm_feature

            # if the second value in the CDM_feature row is 'TWF' ---> it means that it should be in the cdm_twf database
            if cdm_feature[1] == "TWF":
                # Add the fid
                sql_string = sql_string + "\n" + "alter table cdm_twf add column " + \
                str(cdm_feature[0]) + " " + str(cdm_feature[2]).lower() + ";"
                # Add the fid_c
                sql_string = sql_string + "\n" + "alter table cdm_twf add column " + \
                str(cdm_feature[0]) + "_c int;"

            with open('add_feature.sql', 'wb') as f:
                f.write(sql_string)
            bash_string = "psql -d %s -a -f add_feature.sql" % dashan_id
            return_code = os.system(bash_string)
            # Start derive the feature
            answer = raw_input("Do you want to start derive the data for fid %s to %s (y/n): "\
                 % (fid, datalink_id))
            if answer != 'y':
                quit(0)
            else:
                answer = raw_input("Do you want to update all features dependening on fid %s (y/n)" % fid)
                if answer != 'y':
                    print "Derive feature %s" % fid
                    bash_string = "python derive.py %s %s" % (datalink_id, fid)
                    os.system(bash_string)
                else:
                    print "Derive feature %s and all its dependants" % fid
                    bash_string = "python derive.py %s %s dependent" % (datalink_id, fid)
                    os.system(bash_string)

        else:
            print "Warning: no derive function"