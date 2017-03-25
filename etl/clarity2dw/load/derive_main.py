"""
derive_main.py
The main function to generate values for all derived features
"""
from resources import CDM
from dashan_config import Config
import function.derive as derive_func


def get_derive_seq(features=None, input_map=None):
    """
    features is a list of dictionaries
    """
    def rm_measured_dependencies(row, df_list):
        lst = map(str.lstrip, map(str.strip, row.split(',')))
        return [x for x in lst if x in df_list]

    def reduce_dependencies(dependency_lst):
        return [x for x in dependency_lst if x not in order]

    order = []

    if input_map:
        d_map = input_map
    else:
        # create the dependency map
        d_map = dict((feature['fid'], feature['derive_func_input']) \
            for feature in features if ((not feature['is_measured']) \
            and (not feature['is_deprecated'])))
    derived_features = d_map.keys()

    # clear out dependencies on measured features, they should be in CDM already
    d_map = dict((k,rm_measured_dependencies(v, derived_features)) \
        for (k,v) in d_map.iteritems())

    while (len(d_map) != 0):
        ind =  [k for k in d_map if len(d_map[k]) == 0]
        order.extend(ind)
        d_map = dict((k,v) for (k,v) in d_map.iteritems() if k not in order)
        d_map = dict((k, reduce_dependencies(v)) for (k, v) in d_map.iteritems())
    return order

def get_dependent_features(feature_list, features):
    # create the dependency map
    d_map = dict((feature['fid'], feature['derive_func_input']) \
            for feature in features if ((not feature['is_measured']) \
            and (not feature['is_deprecated'])))
    derived_features = d_map.keys()
    get_dependents = feature_list
    dependency_list = []
    first = True

    while len(get_dependents) != 0:
        if first:
            first = False
        else:
            dependency_list.append(get_dependents)
        get_dependents = [fid for fid in derived_features if \
            any(map(lambda x: x in [item.strip() for item \
                in d_map[fid].split(",")], get_dependents))]
        # get_dependents = [fid in derived_features if \
        #     any(map(lambda x: x in d_map[fid], get_dependents))]

    dependent_features = [item for lst in dependency_list for item in lst]
    if len(dependent_features) ==  0:
        return dependent_features
    else:
        dic = dict((feature['fid'], feature['derive_func_input']) \
              for feature in features if feature['fid'] in dependent_features)
        return get_derive_seq(input_map=dic)

def derive_feature(feature, cdm, twf_table='cdm_twf'):
    fid = feature['fid']
    derive_func_id = feature['derive_func_id']
    derive_func_input = feature['derive_func_input']
    cdm.log.info("derive feature %s, function %s, inputs (%s)" \
        % (fid, derive_func_id, derive_func_input))
    derive_func.derive(fid, derive_func_id, derive_func_input, cdm, twf_table)
    cdm.log.info("derive feature %s end." % fid)

def derive_main(dashan, mode=None, fid=None):
    if isinstance(dashan, str):
        # get all derive features
        config = Config(dashan)
        cdm = CDM(config)
    elif isinstance(dashan, CDM):
        cdm = dashan
    cdm.connect()
    features = cdm.get_all_cdm_features()
    feature_map = dict((feature['fid'], feature) for feature in features)
    # generate a sequence to derive
    derive_feature_order = get_derive_seq(features)
    print derive_feature_order
    if mode == 'append':
        append = fid
        cdm.log.info("starts from feature %s" % append)
        idx = derive_feature_order.index(append)
        for i in range(idx, len(derive_feature_order)):
            fid = derive_feature_order[i]
            derive_feature(feature_map[fid], cdm)
    elif mode == 'dependent':
        dependent = fid
        if not feature_map[fid]['is_measured']:
            cdm.log.info("update feature %s and its dependents" % dependent)
            derive_one_feature(dashan, fid)
        else:
            cdm.log.info("update feature %s's dependents" % dependent)
        derive_feature_order = get_dependent_features([dependent], features)
        for fid in derive_feature_order:
            derive_feature(feature_map[fid], cdm)
    elif mode is None and fid is None:
        print "derive features one by one"
        for fid in derive_feature_order:
            derive_feature(feature_map[fid], cdm)
    else:
        print "Unknown mode!"
    if isinstance(dashan, str):
        cdm.disconnect()

def print_derive_feature_sequence(dashan):
    config = Config(dashan)
    cdm = CDM(config)
    cdm.connect()
    features = cdm.get_all_cdm_features()
    feature_map = dict((feature['fid'], feature) for feature in features)
    # generate a sequence to derive
    derive_feature_order = get_derive_seq(features)
    print derive_feature_order
    cdm.disconnect()

def print_feature_list_and_dependents(feature_list):
    config = Config('derive')
    cdm = CDM(config)
    cdm.connect()
    features = cdm.get_all_cdm_features()
    # generate a sequence to derive
    derive_feature_order = get_dependent_features(feature_list, features)
    print derive_feature_order
    cdm.disconnect()


def derive_one_feature(dashan, fid):
    # get all derive features
    config = Config(dashan)
    cdm = CDM(config)
    cdm.connect()
    features = cdm.get_all_cdm_features()
    feature_map = dict((feature['fid'], feature) for feature in features)

    # derive feature
    derive_feature(feature_map[fid], cdm)
    cdm.disconnect()


if __name__ == '__main__':
    derive_main()
