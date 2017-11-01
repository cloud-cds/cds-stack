# Custom antibiotics per hospital

bmc_jhh_icu_disease_antibiotics = [
  {'display_name': 'Pneumonia - Community-Acquired/Early VAP',      'key': '20' },
  {'display_name': 'Pneumonia - Late VAP or Healthcare Associated', 'key': '21' },
  {'display_name': 'Urosepsis',                                     'key': '22' },
  {'display_name': 'Intra-abdominal Infection',                     'key': '23' },
  {'display_name': 'Sepsis of Unclear Source',                      'key': '24' },
  #{'display_name': 'MRSA Coverage',                                 'key': '24' },
]

bmc_jhh_ed_disease_antibiotics = [
  {'display_name': 'CAP/Non-MRSA Pseudomonas',                      'key': '8' },
  {'display_name': 'HAP/VAP or CAP Pseudomonas',                    'key': '9' },
  {'display_name': 'Urosepsis',                                     'key': '10' },
  {'display_name': 'Intra-abdominal Infection',                     'key': '11' },
  {'display_name': 'Unclear Source',                                'key': '6' },
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

############################################################
# TODO: CRITICAL: rebuild due to HCGH launch changes as of 11/1/2017 for Epic 2014.
order_key_urls = {
  '2'  : 'http://OrderTemplate?Name=Lactic+Acid%2C+Plasma|OTLID=3679441|ORXID=116652|OSQID=|ORDMODE=2|KEY=2|DEFAULTS=|'                                                        ,
  '4'  : 'http://OrderTemplate?Name=BLOOD+CULTURES+X2|OTLID=3679442|ORXID=367066|OSQID=201789|ORDMODE=2|KEY=4|DEFAULTS=|'                                                      ,
  '1'  : 'http://OrderTemplate?Name=Sepsis+IV+Fluids|OTLID=3679456|ORXID=383233|OSQID=211120|ORDMODE=2|KEY=1|DEFAULTS=|'                                                       ,
  '3'  : 'http://OrderTemplate?Name=Sepsis+IV+Antibiotics|OTLID=3679463|ORXID=383234|OSQID=305114|ORDMODE=2|KEY=3|DEFAULTS=|'                                                  ,
  '7'  : 'http://OrderTemplate?Name=Sepsis+Vasoactive+Medications|OTLID=3679472|ORXID=383235|OSQID=305115|ORDMODE=2|KEY=7|DEFAULTS=|'                                          ,
  '13' : 'http://OrderTemplate?Name=BMC+TREWS+Vasopressors|OTLID=3679535|ORXID=383242|OSQID=305122|ORDMODE=2|KEY=13|DEFAULTS=|'                                                ,
  '6'  : 'http://OrderTemplate?Name=BMC+TREWS+Antibiotics+-+Pneumonia+-+Community-Acquired%2FEarly+VAP|OTLID=3679530|ORXID=383236|OSQID=305116|ORDMODE=2|KEY=6|DEFAULTS=|'     ,
  '8'  : 'http://OrderTemplate?Name=BMC+TREWS+Antibiotics+-+Pneumonia+-+Late+VAP+or+Healthcare+Associated|OTLID=2654473|ORXID=383237|OSQID=305117|ORDMODE=2|KEY=8|DEFAULTS=|'  ,
  '9'  : 'http://OrderTemplate?Name=BMC+TREWS+Antibiotics+-+MRSA+Coverage|OTLID=3679531|ORXID=383238|OSQID=305118|ORDMODE=2|KEY=9|DEFAULTS=|'                                  ,
  '10' : 'http://OrderTemplate?Name=BMC+TREWS+Antibiotics+-+Urosepsis|OTLID=3679532|ORXID=383239|OSQID=305119|ORDMODE=2|KEY=10|DEFAULTS=|'                                     ,
  '11' : 'http://OrderTemplate?Name=BMC+TREWS+Antibiotics+-+Intra-abdominal+Infection|OTLID=3679533|ORXID=383240|OSQID=305120|ORDMODE=2|KEY=11|DEFAULTS=|'                     ,
  '12' : 'http://OrderTemplate?Name=BMC+TREWS+Antibiotics+-+Sepsis+of+Unclear+Source|OTLID=3679534|ORXID=383241|OSQID=305121|ORDMODE=2|KEY=12|DEFAULTS=|'                      ,
}