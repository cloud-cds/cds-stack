# Custom antibiotics per hospital

bmc_jhh_icu_disease_antibiotics = [
  {'display_name': 'Pneumonia - Community-Acquired/Early VAP',      'key': 6  },
  {'display_name': 'Pneumonia - Late VAP or Healthcare Associated', 'key': 8  },
  {'display_name': 'MRSA Coverage',                                 'key': 9  },
  {'display_name': 'Urosepsis',                                     'key': 10 },
  {'display_name': 'Intra-abdominal Infection',                     'key': 11 },
  {'display_name': 'Sepsis of Unclear Source',                      'key': 12 },
]

bmc_jhh_ed_disease_antibiotics = [
  {'display_name': 'Pneumonia - Community-Acquired/Early VAP',      'key': 6  },
  {'display_name': 'Pneumonia - Late VAP or Healthcare Associated', 'key': 8  },
  {'display_name': 'MRSA Coverage',                                 'key': 9  },
  {'display_name': 'Urosepsis',                                     'key': 10 },
  {'display_name': 'Intra-abdominal Infection',                     'key': 11 },
  {'display_name': 'Sepsis of Unclear Source',                      'key': 12 },
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
    '110107052': 'ICU',   # JHH WEINBERG 3A
    '110107058': 'ICU',   # JHH WEINBERG 5C
    '110107063': 'ICU',   # JHH ZAYED 3W
    '110107064': 'ICU',   # JHH BLOOMBERG 4S
    '110107065': 'ICU',   # JHH ZAYED 5E
    '110107067': 'ICU',   # JHH ZAYED 5W
    '110107070': 'ICU',   # JHH ZAYED 9E
    '110107075': 'ICU',   # JHH ZAYED 10E
    '110107126': 'ICU',   # JHH BLOOMBERG 8N
    '110107422': 'ED',    # JHH EMERGENCY MEDICINE
    '110107423': 'ED',    # JHH PEDS ED
  },
  'BMC': {
    '110203001': 'ICU',   # BMC NICU
    '110203010': 'ICU',   # BMC BURN ICU
    '110203014': 'ICU',   # BMC CARDIAC ICU
    '110203022': 'ICU',   # BMC MEDICAL ICU
    '110203023': 'ICU',   # BMC NEUROSCIENCE CCU
    '110203029': 'ICU',   # BMC SURGICAL ICU
    '110203422': 'ED',    # BMC PEDS EMERGENCY DT
    '110203424': 'ED',    # BMC EMERGENCY SERVICES
  },
  'HCGH': {
    '110300170': 'ICU',   # HCGH 3C ICU
    '110300220': 'ICU',   # HCGH 2C NICU
    '110300460': 'ED',    # HCGH EMERGENCY-ADULTS
    '110300470': 'ED',    # HCGH EMERGENCY-PEDS
  },
  'SMH': {
    '110400001': 'ICU',   # SMH 3B SCN
    '110400240': 'ICU',   # SMH INTENSIVE CARE
    '110400321': 'ED',    # SMH EMERGENCY DEPARTMENT   
  },
  'SH': {
    '110500007': 'ICU',   # SH INTENSIVE CARE 3100
    '110500008': 'ICU',   # SH INTENSIVE CARE 3400
    '110500002': 'ED',    # SH CLIN DECISION 6400
    '110500013': 'ED',    # SH EMERGENCY MAIN
    '110500014': 'ED',    # SH EMERGENCY PEDS
  }
}
