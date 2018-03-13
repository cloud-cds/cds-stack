"""
diag, hist, and prob are not included in this cdm_defs
"""
cdm_defs = {
    'abp': {
        'unit':     'mmHg',
        'value':    str,
        'thresh':   (None, None),
        'human_readable':   'Arterial Blood Pressure',
    },
    'abp_dias': {
        'unit':     'mmHg',
        'value':    float,
        'thresh':   (8, 150),
        'human_readable':   'Diastolic Arterial BP',
    },
    'abp_sys': {
        'unit':     'mmHg',
        'value':    float,
        'thresh':   (30, 250),
        'human_readable':   'Systolic Arterial BP',
    },
    'albumin_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Albumin',
    },
    'acebutolol_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Acebutolol',
    },
    'aminoglycosides_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Aminoglycosides',
    },
    'alt_liver_enzymes': {
        'unit':     'Units/L',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'ALT Liver Enzymes',
    },
    'amlodipine_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Amlodipine',
    },
    'amoxicillin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Amoxicillin',
    },
    'ampicillin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Ampicillin',
    },
    'amylase': {
        'unit':     'Units/L',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Amylase',
    },
    'arterial_ph': {
        'unit':     '',
        'value':    float,
        'thresh':   (6.5, 8.5),
        'human_readable':   'Arterial pH',
    },
    'ast_liver_enzymes': {
        'unit':     'Units/L',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'AST Liver Enzymes',
    },
    'atenolol_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Atenolol',
    },
    'atorvastatin_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Atorvastatin',
    },
    'azithromycin_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Azithromycin',
    },
    'aztreonam_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Aztreonam',
    },
    'bands': {
        'unit':     '%',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Bands',
    },
    'benazepril_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Benazepril',
    },
    'bilirubin': {
        'unit':     'mg/dL',
        'value':    float,
        'thresh':   (0, 50),
        'human_readable':   'Bilirubin',
    },
    'bicarbonate': {
        'unit':     'mmol/L',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Bicarbonate',
    },
    'bipap': {
        'unit':     '',
        'value':    None,
        'thresh':   (None, None),
        'human_readable':   'Bipap',
    },
    'bisoprolol_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Bisoprolol',
    },
    'blood_culture': {
        'unit':     '',
        'value':    None,
        'thresh':   (None, None),
        'human_readable':   'Blood Cultures',
    },
    'bun': {
        'unit':     'mg/dL',
        'value':    float,
        'thresh':   (0.1, 180),
        'human_readable':   'BUN',
    },
    'bun_to_cr': {
        'unit':     'Ratio',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'BUN to Creatinine Ratio',
    },
    'captopril_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Captopril',
    },
    'cefazolin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Cefazolin',
    },
    'cefepime_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Cefepime',
    },
    'ceftazidime_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Ceftazidime',
    },
    'ceftriaxone_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Ceftriaxone',
    },
    'cephalosporins_1st_gen_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Cephalosporins 1ST Generation',
    },
    'cephalosporins_2nd_gen_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Cephalosporins 2ND Generation',
    },
    'ciprofloxacin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Ciprofloxacin',
    },
    'clindamycin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Clindamycin',
    },
    'cms_antibiotics': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Sepsis Antibiotics Bundle',
    },
    'co2': {
        'unit':     'mmol/L',
        'value':    float,
        'thresh':   (0.1, 55),
        'human_readable':   'CO2',
    },
    'cpap': {
        'unit':     '',
        'value':    None,
        'thresh':   (None, None),
        'human_readable':   'Cpap',
    },
    'creatinine': {
        'unit':     'mg/dL',
        'value':    float,
        'thresh':   (0.1, 40),
        'human_readable':   'Creatinine',
    },
    'crystalloid_fluid': {
        'unit':     'ml',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Total Crystalloid Fluid',
    },
    'daptomycin_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Dopamine'
    },
    'dextrose_water': {
        'unit':     'ml',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Dextrose Water',
    },
    'dextrose_normal_saline': {
        'unit':     'ml',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Dextrose Normal Saline',
    },
    'ddimer': {
        'unit':     'mg/L FEU',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'D-Dimer'
    },
    'dialysis': {
        'unit':     'ml',
        'value':    float,
        'thresh':   (0.01, None),
        'human_readable':   'Dialysis'
    },
    'dobutamine_dose': {
        'unit':     'mcg/kg/min',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Dobutamine',
    },
    'diltiazem_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Diltiazem',
    },
    'dopamine_dose': {
        'unit':     'mcg/kg/min',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Dopamine',
    },
    'enalapril_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Epinephrine',
    },
    'enalaprilat_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Epinephrine',
    },
    'epinephrine_dose': {
        'unit':     'mcg/kg/min',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Epinephrine',
    },
    'erythromycin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Erythromycin',
    },
    'ezetimibe_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Ezetimibe',
    },
    'fio2': {
        'unit':     '%',
        'value':    float,
        'thresh':   (20, 100),
        'human_readable':   'FiO2',
    },
    'fluids_intake': {
        'unit':     'ml',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Total Fluid Intake',
    },
    'gcs': {
        'unit':     '',
        'value':    float,
        'thresh':   (3, 15),
        'human_readable':   'GCS',
    },
    'gentamicin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Gentamicin',
    },
    'glycopeptides_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Glycopeptides'
    },
    'heart_rate': {
        'unit':     'beats per min',
        'value':    float,
        'thresh':   (20, 300),
        'human_readable':   'Heart Rate',
    },
    'hemoglobin': {
        'unit':     'g/dL',
        'value':    float,
        'thresh':   (4, 20),
        'human_readable':   'Hemoglobin',
    },
    'heparin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Heparin',
    },
    'hetastarch': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Hetastarch',
    },
    'inr': {
        'unit':     'Ratio',
        'value':    float,
        'thresh':   (0.01, 12),
        'human_readable':   'INR',
    },
    'lactate': {
        'unit':     'mmol/L',
        'value':    float,
        'thresh':   (0.2, 40),
        'human_readable':   'Lactate',
    },
    'lactated_ringers': {
        'unit':     'ml',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Lactated Ringers',
    },
    'levofloxacin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Levofloxacin',
    },
    'levophed_infusion_dose': {
        'unit':     'mcg/kg/min',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Levophed',
    },
    'lipase': {
        'unit':     'Units/L',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Lipase',
    },
    'lisinopril_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Lisinopril',
    },
    'linezolid_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Linezolid'
    },
    'macrolides_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Macrolides'
    },
    'map': {
        'unit':     'mmHg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'MAP',
    },
    'mapm': {
        'unit':     'mmHg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'MAPM',
    },
    'meropenem_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Meropenem',
    },
    'metoprolol_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Metoprolol',
    },
    'metronidazole_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Metronidazole',
    },
    'milrinone_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Milrinone',
    },
    'moxifloxacin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Moxifloxacin',
    },
    'nadolol_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Moxifloxacin',
    },
    'nbp': {
        'unit':     'mmHg',
        'value':    str,
        'thresh':   (None, None),
        'human_readable':   'Blood Pressure',
    },
    'nbp_dias': {
        'unit':     'mmHg',
        'value':    float,
        'thresh':   (8, 150),
        'human_readable':   'Non-invasive Diastolic BP',
    },
    'nbp_sys': {
        'unit':     'mmHg',
        'value':    float,
        'thresh':   (30, 250),
        'human_readable':   'Non-invasive Systolic BP',
    },
    'neosynephrine_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Neo-Synephrine',
    },
    'nicardipine_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Nicardipine',
    },
    'nifedipine_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Nifedipine',
    },
    'norepinephrine_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Norepinephrine',
    },
    'oxacillin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Oxacillin',
    },
    'paco2': {
        'unit':     'mmHg',
        'value':    float,
        'thresh':   (5, 100),
        'human_readable':   'PaCO2',
    },
    'pao2': {
        'unit':     'mmHg',
        'value':    float,
        'thresh':   (0.1, 500),
        'human_readable':   'PaO2',
    },
    'penicillin_dose': {
        'unit':     'Million Units',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Penicillin',
    },
    'penicillin_g_dose': {
        'unit':     'Million Units',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Penicillin G',
    },
    'piperacillin_tazobac_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Piperacillin/tazobactam',
    },
    'platelets': {
        'unit':     '1000/uL',
        'value':    float,
        'thresh':   (0.1, 1200),
        'human_readable':   'Platelets',
    },
    'ptt': {
        'unit':     'Seconds',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Partial thromboplastin time (PTT)',
    },
    'pravastatin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Pravastatin',
    },
    'propofol_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Propofol',
    },
    'propranolol_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Propofol',
    },
    'rapamycin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Rapamycin',
    },
    'rass': {
        'unit':     '',
        'value':    str,
        'thresh':   (None, None),
        'human_readable':   'RASS',
    },
    'resp_rate': {
        'unit':     'breath per min',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Respiratory Rate',
    },
    'rifampin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Rifampin',
    },
    'rosuvastatin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Rosuvastatin',
    },
    'simvastatin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Simvastatin',
    },
    'sodium': {
        'unit':     'mmol/L',
        'value':    float,
        'thresh':   (115, 160),
        'human_readable':   'Sodium',
    },
    'sodium_bicarbonate': {
        'unit':     'ml',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Sodium Bicarbonate',
    },
    'sodium_chloride': {
        'unit':     'ml',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Saline',
    },
    'spo2': {
        'unit':     '%',
        'value':    float,
        'thresh':   (0, 100),
        'human_readable':   'SpO2',
    },
    'tobramycin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Tobramycin',
    },
    'temperature': {
        'unit':     'Fahrenheit',
        'value':    float,
        'thresh':   (80, 110),
        'human_readable':   'Temperature',
    },
    'tobramycin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Tobramycin',
    },
    'urine_output': {
        'unit':     'ml',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Urine Output',
    },
    'vancomycin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Vancomycin',
    },
    'vasopressin_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Vasopressin',
    },
    'vasopressors_dose': {
        'unit':     '',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Vasopressors',
    },
    'vent': {
        'unit':     '',
        'value':    str,
        'thresh':   (None, None),
        'human_readable':   'Mechanical Ventilation',
    },
    'mech_vent': {
        'unit':     '',
        'value':    str,
        'thresh':   (None, None),
        'human_readable':   'Mechanical Ventilation',
    },
    'verapamil_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Verapamil',
    },
    'warfarin_dose': {
        'unit':     'mg',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Warfarin',
    },
    'wbc': {
        'unit':     '1000/uL',
        'value':    float,
        'thresh':   (0.1, 70),
        'human_readable':   'WBC',
    },
    'weight': {
        'unit':     'kg',
        'value':    float,
        'thresh':   (0.1, 300),
        'human_readable':   'Weight',
    },
    'influanat': {
        'unit':     '',
        'value':    str,
        'thresh':   (None, None),
        'human_readable':   'Influanat',
    },
    'influbnat': {
        'unit':     '',
        'value':    str,
        'thresh':   (None, None),
        'human_readable':   'Influbnat',
    },
    'rsvnat': {
        'unit':     '',
        'value':    str,
        'thresh':   (None, None),
        'human_readable':   'rsvnat',
    },
    'calcium': {
        'unit':     'mg/dL',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Partial thromboplastin time (PTT)',
    },
    'magnesium': {
        'unit':     'mg/dL',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Magnesium',
    },
    'troponin': {
        'unit':     'ng/mL',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Troponin',
    },
    'hematocrit': {
        'unit':     '%',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Hematocrit',
    },
    'potassium': {
        'unit':     'mmol/L',
        'value':    float,
        'thresh':   (None, None),
        'human_readable':   'Potassium',
    },
    'rivastigmine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Rivastigmine'},
    'pantoprazole_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Pantoprazole'},
    'adenosine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Adenosine'},
    'haloperidol_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Haloperidol'},
    'quetiapine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Quetiapine'},
    'olanzapine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Olanzapine'},
    'risperidone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Risperidone'},
    'trifluoperazine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Trifluoperazine'},
    'valproic_acid_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Valproic_acid'},
    'lithium_citrate_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Lithium_citrate'},
    'lithium_carbonate_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Lithium_carbonate'},
    'benztropine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Benztropine'},
    'donepezil_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Donepezil'},
    'galantamine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Galantamine'},
    'memantine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Memantine'},
    'memantine_donepezil_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Memantine_donepezil'},
    'levetiracetam_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Levetiracetam'},
    'anagrelide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Anagrelide'},
    'aspirin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Aspirin'},
    'clopidogrel_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Clopidogrel'},
    'prasugrel_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Prasugrel'},
    'ticagrelor_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Ticagrelor'},
    'tirofiban_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Tirofiban'},
    'vorapaxar_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Vorapaxar'},
    'dipyridamole_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Dipyridamole'},
    'disopyramide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Disopyramide'},
    'mexiletine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Mexiletine'},
    'quinidine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Quinidine'},
    'procainamide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Procainamide'},
    'propafenone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Propafenone'},
    'flecainide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Flecainide'},
    'betaxolol_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Betaxolol'},
    'labetolol_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Labetolol'},
    'nebivolol_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Nebivolol'},
    'penbutolol_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Penbutolol'},
    'timolol_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Timolol'},
    'pindolol_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Pindolol'},
    'dronedarone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Dronedarone'},
    'edoxaban_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Edoxaban'},
    'enoxaparin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Enoxaparin'},
    'dalteparin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Dalteparin'},
    'fondaparinux_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Fondaparinux'},
    'benazepril_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Benazepril'},
    'captopril_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Captopril'},
    'fosinopril_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Fosinopril'},
    'moexipril_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Moexipril'},
    'perindopril_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Perindopril'},
    'quinapril_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Quinapril'},
    'trandolapril_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Trandolapril'},
    'brivaracetam_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Brivaracetam'},
    'carbamazepine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Carbamazepine'},
    'clobazam_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Clobazam'},
    'clonazepam_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Clonazepam'},
    'divalproex_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Divalproex'},
    'eslicarbazepine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Eslicarbazepine'},
    'ethosuximide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Ethosuximide'},
    'ethotoin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Ethotoin'},
    'ezogabine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Ezogabine'},
    'felbamate_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Felbamate'},
    'fosphenytoin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Fosphenytoin'},
    'gabapentin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Gabapentin'},
    'lacosamide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Lacosamide'},
    'lamotrigine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Lamotrigine'},
    'mephenytoin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Mephenytoin'},
    'mephobarbital_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Mephobarbital'},
    'methsuximide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Methsuximide'},
    'oxcarbazepine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Oxcarbazepine'},
    'paramethadione_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Paramethadione'},
    'perampanel_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Perampanel'},
    'phenacemide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Phenacemide'},
    'pregabalin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Pregabalin'},
    'primidone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Primidone'},
    'rufinamide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Rufinamide'},
    'tiagabine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Tiagabine'},
    'trimethadione_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Trimethadione'},
    'valproate_sodium_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Valproate_sodium'},
    'vigabatrin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Vigabatrin'},
    'acarbose_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Acarbose'},
    'acetohexamide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Acetohexamide'},
    'albiglutide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Albiglutide'},
    'alogliptin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Alogliptin'},
    'bromocriptine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Bromocriptine'},
    'canagliflozin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Canagliflozin'},
    'chlorpropamide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Chlorpropamide'},
    'dapagliflozin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Dapagliflozin'},
    'dulaglutide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Dulaglutide'},
    'empagliflozin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Empagliflozin'},
    'exenatide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Exenatide'},
    'glimepiride_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Glimepiride'},
    'glipizide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Glipizide'},
    'glyburide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Glyburide'},
    'linagliptin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Linagliptin'},
    'liraglutide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Liraglutide'},
    'lixisenatide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Lixisenatide'},
    'mifepristone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Mifepristone'},
    'miglitol_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Miglitol'},
    'nateglinide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Nateglinide'},
    'pioglitazone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Pioglitazone'},
    'pramlintide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Pramlintide'},
    'repaglinide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Repaglinide'},
    'rosiglitazone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Rosiglitazone'},
    'saxagliptin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Saxagliptin'},
    'sitagliptin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Sitagliptin'},
    'tolazamide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Tolazamide'},
    'tolbutamide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Tolbutamide'},
    'troglitazone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Troglitazone'},
    'amiloride_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Amiloride'},
    'ammonium_chloride_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Ammonium_chloride'},
    'bendroflumethiazide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Bendroflumethiazide'},
    'chlorthalidone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Chlorthalidone'},
    'conivaptan_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Conivaptan'},
    'dichlorphenamide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Dichlorphenamide'},
    'eplerenone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Eplerenone'},
    'ethacrynate_sodium_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Ethacrynate_sodium'},
    'ethacrynic_acid_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Ethacrynic_acid'},
    'hydroflumethiazide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Hydroflumethiazide'},
    'methazolamide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Methazolamide'},
    'methyclothiazide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Methyclothiazide'},
    'metolazone_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Metolazone'},
    'pamabrom_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Pamabrom'},
    'polythiazide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Polythiazide'},
    'tolvaptan_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Tolvaptan'},
    'torsemide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Torsemide'},
    'trichlormethiazide_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Trichlormethiazide'},
    'flumazenil_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Flumazenil'},
    'nitroglycerin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Nitroglycerin'},
    'nitroprusside_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Nitroprusside'},
    'phentolamine_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Phentolamine'},
    'digoxin_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Digoxin'},
    'magnesium_sulfate_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Magnesium_sulfate'},
    'alteplase_dose': {'unit':'mg', 'value':float, 'thresh': (None, None), 'human_readable':'Alteplase'},
    'consult': {'unit':'', 'value': str, 'thresh': (None, None), 'human_readable':'consult'},
    'ircv_case_request': {'unit':'', 'value': str, 'thresh': (None, None), 'human_readable':'ircv_case_request'},
    'angiogram_order': {'unit':'', 'value': str, 'thresh': (None, None), 'human_readable':'angiogram_order'},
}
