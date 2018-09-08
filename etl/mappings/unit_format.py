translation_map = [
# Time
    ('Seconds',         ['seconds', 'sec',]),
# Pressure
    ('mmHg',            ['mmhg', 'mm[hg]']),
# Temperature
    ('Celcius',         ['celcius', 'c', '°c']),
    ('Fahrenheit',      ['fahrenheit']),
# Other
    ('Units/L',         ['u/l', 'iu/l', 'units/l']),
    ('%',               ['%', 'percent']),
    ('Ratio',           ['ratio']),
    ('1000/uL',         ['1000/ul', 'k/cumm', 'x10e3/ul', 'k/mm3', 'thousand/ul', 'thou/ul']),
    ('Units',           ['units']),
    ('Million Units',   ['million units']),
# Volume
    ('ml',              ['ml']),
# Mass
    ('mcg',             ['mcg']),
    ('mg',              ['mg', 'mg of piperacillin', 'mg of ampicillin',
                         'mg of amoxicillin']),
    ('g',               ['g', 'gm', 'gram']),
    ('kg',              ['kg', 'kilogram', 'kilograms']),
# Concentration
    ('mg/dL',           ['mg/dl']),
    ('g/dL',            ['gm/dl', 'g/dl']),
    ('g/L',             ['gm/l', 'g/l']),
    ('mmol/L',          ['mmol/l', 'meq/l']),
# Rate: Volume/time
    ('ml/hr',           ['ml/hr']),
    ('mcg/kg/min',      ['mcg/kg/min']),
# Rate: Mass/time
    ('mcg/min',         ['mcg/min']),
    ('mcg/hr',          ['mcg/hr']),
    ('mg/min',          ['mg/min']),
    ('g/hr',            ['g/hr']),
# Rate: other
    ('beats per min',   ['beats per min']),
    ('breath per min',  ['breath per min', 'breaths per min']),
# Medicine dose
    ('Dose',            ['dose']),
    ('mg/kg/dose',      ['mg/kg/dose', 'mg/kg/dose of ampicillin']),
    ('tablet',          ['tablet']),
# DDimer
    ('mg/L',            ['mg/l']),
    ('mg/L FEU',        ['mg/l feu']),
# Troponin
    ('ng/mL',           ['ng/ml'])
]

empty_translation_map = {
    'heart_rate':       'beats per min',
    'resp_rate':        'breath per min',
    'map':              'mmHg',
    'temperature':      'Celcius',          # Nurses sometimes forget this
}
