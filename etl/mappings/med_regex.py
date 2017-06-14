med_regex = [
    {
        'fid': 'albumin_dose',
        'pos': '^albumin human \(PLASBUMIN\)',
        'neg': 'injection|flush syringe|nebulizer',
        # 'part_of': ['fluids_intake'],
    }, {
        'fid': 'amoxicillin_dose',
        'pos': '^amoxicillin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'ampicillin_dose',
        'pos': '^ampicillin',
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
        'fid': 'ciprofloxacin_dose',
        'pos': '^ciprofloxacin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'clindamycin_dose',
        'pos': '^clindamycin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop'
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
        'neg':  'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'dopamine_dose',
        'pos': 'dopamine',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'epinephrine_dose',
        'pos': '^epinephrine',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'erythromycin_dose',
        'pos': '^erythromycin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'gentamicin_dose',
        'pos': '^gentamicin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
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
        'pos': 'levophed.+infusion|^norepinephrine',
        'neg': 'injection|gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'meropenem_dose',
        'pos': '^meropenem',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'metronidazole_dose',
        'pos': '^metronidazole',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'milrinone_dose',
        'pos': '^milrinone',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'moxifloxacin_dose',
        'pos': '^moxifloxacin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'neosynephrine_dose',
        'pos': 'phenylephrine.*neo-synephrine',
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
        'fid': 'piperacillin_tazbac_dose',
        'pos': '^piperacillin-tazobactam',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'rifampin_dose',
        'pos': '^rifampin',
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
        'fid': 'vancomycin_dose',
        'pos': '^vancomycin',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
        'part_of': ['cms_antibiotics'],
    }, {
        'fid': 'vasopressin_dose',
        'pos': '^dobutamin|^dopamine|^epinephrine infusion|^milrinone|^norepinephrine infusion|vasopressin infusion',
        'neg': '',
    },
    # {
    #     'fid': 'fluids_intake',
    #     'pos': '^albumin human \(PLASBUMIN\)|^dextrose 5% lactated ringers bolus|^hetastarch|^lactated ringers|^sodium chloride 0.9( %|%) (?!injection|flush syringe|nebulizer)',
    #     'neg': '',
    # },
]
