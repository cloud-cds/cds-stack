med_regex = [
    {
        'fid': 'acebutolol_dose',
        'pos': '^acebutolol',
    }, {
        'fid': 'albumin_dose',
        'pos': '^albumin human \(PLASBUMIN\)',
        'neg': 'injection|flush syringe|nebulizer',
    }, {
        'fid': 'amlodipine_dose',
        'pos': '^amlodipine.*norvasc',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'aminoglycosides_dose',
        'pos': '^(amikacin|amikin|bethkis|garamycin|gentamicin|kanamycin|kantrex|kitabis|nebcin|neo-fradin|neomycin|netilmicin|streptomycin|tobi|tobramycin)'
    }, {
        'fid': 'amoxicillin_dose',
        'pos': '^amoxicillin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'ampicillin_dose',
        'pos': '^ampicillin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'atenolol_dose',
        'pos': '^atenolol',
    }, {
        'fid': 'atorvastatin_dose',
        'pos': '^atorvastatin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'azithromycin_dose',
        'pos': '^azithromycin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'aztreonam_dose',
        'pos': '^aztreonam',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'benazepril_dose',
        'pos': '^benazepril .*LOTENSIN',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'bisoprolol_dose',
        'pos': '^bisoprolol',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'captopril_dose',
        'pos': '^captopril',
    }, {
        'fid': 'cefazolin_dose',
        'pos': '^cefazolin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop'
    }, {
        'fid': 'cefepime_dose',
        'pos': '^cefepime',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'ceftazidime_dose',
        'pos': '^ceftazidime',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'ceftriaxone_dose',
        'pos': '^ceftriaxone',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'cephalosporins_1st_gen_dose',
        'pos': '^(ancef|cefadroxil|cefazolin|cephalexin|cephalothin|cephapirin|cephradine|daxbia|duricef|irb|keflex|keftab|kefzol|panixine|velosef)',
    }, {
        'fid': 'cephalosporins_2nd_gen_dose',
        'pos': '^(ceclor|cefaclor|cefamandole|cefonicid|cefotan|cefotetan|cefoxitin|cefprozil|ceftin|cefuroxime|cefzil|kefurox|lorabid|loracarbef|mandol|mefoxin|raniclor|zinacef)',
    }, {
        'fid': 'ciprofloxacin_dose',
        'pos': '^ciprofloxacin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'clindamycin_dose',
        'pos': '^clindamycin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop'
    }, {
        'fid': 'diltiazem_dose',
        'pos': '^(diltiazem|cardizem)',
    }, {
        'fid': 'daptomycin_dose',
        'pos': '^daptomycin',
    }, {
        'fid': 'dextrose_water',
        'pos': '^dextrose (5|10)( %|%) in water',
        'neg': 'syringe|nasal spray|injection|gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        # 'part_of': ['fluids_intake', 'crystalloid_fluid'],
        'part_of': ['crystalloid_fluid']
    }, {
        'fid': 'dextrose_normal_saline',
        'pos': '^dextrose (5|10)( %|%) and (0.2|0.45|0.9)( %|%) NS',
        'neg': 'syringe|nasal spray|injection|gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        # 'part_of': ['fluids_intake', 'crystalloid_fluid'],
        'part_of': ['crystalloid_fluid']
    }, {
        'fid': 'dobutamine_dose',
        'pos': 'dobutamine',
        'part_of': ['vasopressors_dose']
    }, {
        'fid': 'dopamine_dose',
        'pos': 'dopamine',
        'part_of': ['vasopressors_dose']
    }, {
        'fid': 'enalapril_dose',
        'pos': '^enalapril ',
    }, {
        'fid': 'enalaprilat_dose',
        'pos': 'enalaprilat',
    }, {
        'fid': 'epinephrine_dose',
        'pos': '^epinephrine',
        'part_of': ['vasopressors_dose']
    }, {
        'fid': 'erythromycin_dose',
        'pos': '^erythromycin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'ezetimibe_dose',
        'pos': '^ezetimibe',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'gentamicin_dose',
        'pos': '^gentamicin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'glycopeptides_dose',
        'pos': '^(vancocin|dalvanc|orbactiv|vibativ)',
    },{
        'fid': 'heparin_dose',
        'pos': 'heparin'
    }, {
        'fid': 'hetastarch',
        'pos': '^hetastarch',
        'neg': 'injection|flush syringe|nebulizer',
        # 'part_of': ['fluids_intake'],
    }, {
        'fid': 'lactated_ringers',
        'pos': '^lactated ringers|^dextrose 5( %|%) in lactated ringers bolus',
        'neg': 'syringe|nasal spray|injection|gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        # 'part_of': ['fluids_intake', 'crystalloid_fluid'],
        'part_of': ['crystalloid_fluid']
    }, {
        'fid': 'levofloxacin_dose',
        'pos': '^levofloxacin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'levophed_infusion_dose',
        'pos': 'levophed.+infusion',
        'neg': 'injection|gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['vasopressors_dose']
    }, {
        'fid': 'lisinopril_dose',
        'pos': 'lisinopril',
    }, {
        'fid': 'linezolid_dose',
        'pos': '^linezolid',
    }, {
        'fid': 'macrolides_dose',
        'pos': '^(azithromycin|biaxin|clarithromycin|dificid|dirithromycin|dynabac|e\.e\.s\.|e-mycin|ery|eryc|eryped|ery-tab|erythrocin|erythrocot|erythromycin|eryzole|fidaxomicin|pce|pediazole|tao|troleandomycin|zithromax|zmax)'
    }, {
        'fid': 'norepinephrine_dose',
        'pos': '^norepinephrine',
        'part_of': ['vasopressors_dose']
    }, {
        'fid': 'meropenem_dose',
        'pos': '^meropenem',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics']
    }, {
        'fid': 'metoprolol_dose',
        'pos': 'metoprolol',
    }, {
        'fid': 'metronidazole_dose',
        'pos': '^metronidazole',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'milrinone_dose',
        'pos': '^milrinone',
        'part_of': ['vasopressors_dose']
    }, {
        'fid': 'moxifloxacin_dose',
        'pos': '^moxifloxacin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
     }, {
        'fid': 'nadolol_dose',
        'pos': 'nadolol',
    }, {
        'fid': 'neosynephrine_dose',
        'pos': 'phenylephrine.*neo-synephrine',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'nicardipine_dose',
        'pos': '^nicardipine CARDENE',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'nifedipine_dose',
        'pos': 'nifedipine',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'oxacillin_dose',
        'pos': '^oxacillin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'penicillin_dose',
        'pos': '^penicillin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'penicillin_g_dose',
        'pos': '^penicillin G',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'piperacillin_tazobac_dose',
        'pos': '^piperacillin-tazobactam',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'pravastatin_dose',
        'pos': '^pravastatin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'propofol_dose',
        'pos': 'propofol'
    }, {
        'fid': 'propranolol_dose',
        'pos': 'propranolol'
    }, {
        'fid': 'rifampin_dose',
        'pos': '^rifampin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'rosuvastatin_dose',
        'pos': '^rosuvastatin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'simvastatin_dose',
        'pos': '^simvastatin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'sodium_bicarbonate',
        'pos': '^sodium bicarbonate.*infusion$',
        'neg': 'syringe|nasal spray|injection|gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        # 'part_of': ['fluids_intake', 'crystalloid_fluid'],
        'part_of': ['crystalloid_fluid'],
    }, {
        'fid': 'sodium_chloride',
        'pos': '^sodium chloride 0.9( %|%)',
        'neg': 'syringe|nasal spray|injection|gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        # 'part_of': ['fluids_intake', 'crystalloid_fluid'],
        'part_of': ['crystalloid_fluid'],
    }, {
        'fid': 'tobramycin_dose',
        'pos': '^tobramycin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'vancomycin_dose',
        'pos': '^vancomycin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'vasopressin_dose',
        'pos': '^vasopressin',
        'part_of': ['vasopressors_dose']
    }, {
        'fid': 'verapamil_dose',
        'pos': 'verapamil'
    }, {
        'fid': 'warfarin_dose',
        'pos': 'warfarin.* tablet'
    }
    # {
    #     'fid': 'fluids_intake',
    #     'pos': '^albumin human \(PLASBUMIN\)|^dextrose 5% lactated ringers bolus|^hetastarch|^lactated ringers|^sodium chloride 0.9( %|%) (?!injection|flush syringe|nebulizer)',
    # },
]
