# Custom antibiotics per hospital

bmc_jhh_icu_disease_antibiotics = [
  {'display_name': 'medication name',      'key': 'xxx' },
]

bmc_jhh_ed_disease_antibiotics = [
  {'display_name': 'medication name',                      'key': 'xxx' },
]

bmc_jhh_antibiotics = {
  'action' : 'antibiotics_order',
  'locations': [{
    'name'        : 'ICU',
    'data_trews'  : 'icu',
    'diseases'    : bmc_jhh_icu_disease_antibiotics
  }]
}

bmc_jhh_ed_antibiotics = {
  'action' : 'antibiotics_order',
  'locations': [{
    'name'        : 'ICU',
    'data_trews'  : 'icu',
    'diseases'    : bmc_jhh_icu_disease_antibiotics
  }, {
    'name'        : 'ED',
    'data_trews'  : 'ed',
    'diseases'    : bmc_jhh_ed_disease_antibiotics
  }]
}

departments_by_hospital = {
  'JHH': {
    'id': 'name',
  },
  'BMC': {
    'id': 'name',
  },
  'HCGH': {
    'id': 'name',
  },
  'SMH': {
    'id': 'name',
  },
  'SH': {
    'id': 'name',
  }
}

############################################################
# TODO: CRITICAL: rebuild due to HCGH launch changes as of 11/1/2017 for Epic 2014.
order_key_urls = {
  'key': 'url',
}