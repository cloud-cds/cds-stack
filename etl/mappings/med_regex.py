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
        'pos': '^(amikacin|amikin|bethkis|garamycin|gentamicin|kanamycin|kantrex|kitabis|nebcin|neo-fradin|neomycin|netilmicin|streptomycin|tobi|tobramycin)',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
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
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'cephalosporins_2nd_gen_dose',
        'pos': '^(ceclor|cefaclor|cefamandole|cefonicid|cefotan|cefotetan|cefoxitin|cefprozil|ceftin|cefuroxime|cefzil|kefurox|lorabid|loracarbef|mandol|mefoxin|raniclor|zinacef)',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
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
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
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
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
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
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
    }, {
        'fid': 'macrolides_dose',
        'pos': '^(azithromycin|biaxin|clarithromycin|dificid|dirithromycin|dynabac|e\.e\.s\.|e-mycin|ery|eryc|eryped|ery-tab|erythrocin|erythrocot|erythromycin|eryzole|fidaxomicin|pce|pediazole|tao|troleandomycin|zithromax|zmax)',
        'neg': 'gel|vaginal|cream|ophthalmic|ointment|nebulizer|drop',
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
    },
    {'fid':'rivastigmine_dose', 'pos':'rivastigmine'},
    {'fid':'pantoprazole_dose', 'pos':'pantoprazole'},
    {'fid':'adenosine_dose', 'pos':'adenosine'},
    {'fid':'haloperidol_dose', 'pos':'haldol'},
    {'fid':'quetiapine_dose', 'pos':'quetiapine'},
    {'fid':'olanzapine_dose', 'pos':'olanzapine'},
    {'fid':'risperidone_dose', 'pos':'risperidone'},
    {'fid':'trifluoperazine_dose', 'pos':'trifluoperazine'},
    {'fid':'valproic_acid_dose', 'pos':'valproic acid'},
    {'fid':'lithium_citrate_dose', 'pos':'lithium citrate'},
    {'fid':'lithium_carbonate_dose', 'pos':'lithium carbonate'},
    {'fid':'benztropine_dose', 'pos':'benztropine'},
    {'fid':'donepezil_dose', 'pos':'^donepezil'},
    {'fid':'galantamine_dose', 'pos':'galantamine'},
    {'fid':'memantine_dose', 'pos':'namenda'},
    {'fid':'memantine_donepezil_dose', 'pos':'memantine-donepezil'},
    {'fid':'levetiracetam_dose', 'pos':'levetiracetam'},
    {'fid':'anagrelide_dose', 'pos':'anagrelide'},
    {'fid':'aspirin_dose', 'pos':'aspirin'},
    {'fid':'clopidogrel_dose', 'pos':'clopidogrel'},
    {'fid':'prasugrel_dose', 'pos':'prasugrel'},
    {'fid':'ticagrelor_dose', 'pos':'ticagrelor'},
    {'fid':'tirofiban_dose', 'pos':'tirofiban'},
    {'fid':'vorapaxar_dose', 'pos':'vorapaxar'},
    {'fid':'dipyridamole_dose', 'pos':'rivastigmine'},
    {'fid':'disopyramide_dose', 'pos':'disopyramide'},
    {'fid':'mexiletine_dose', 'pos':'mexiletine'},
    {'fid':'quinidine_dose', 'pos':'quinidine'},
    {'fid':'procainamide_dose', 'pos':'procainamide'},
    {'fid':'propafenone_dose', 'pos':'propafenone'},
    {'fid':'flecainide_dose', 'pos':'flecainide'},
    {'fid':'betaxolol_dose', 'pos':'betaxolol'},
    {'fid':'labetolol_dose', 'pos':'labetolol'},
    {'fid':'nebivolol_dose', 'pos':'nebivolol'},
    {'fid':'penbutolol_dose', 'pos':'penbutolol'},
    {'fid':'timolol_dose', 'pos':'timolol'},
    {'fid':'pindolol_dose', 'pos':'pindolol'},
    {'fid':'dronedarone_dose', 'pos':'dronedarone'},
    {'fid':'edoxaban_dose', 'pos':'edoxaban'},
    {'fid':'enoxaparin_dose', 'pos':'enoxaparin'},
    {'fid':'dalteparin_dose', 'pos':'dalteparin'},
    {'fid':'fondaparinux_dose', 'pos':'fondaparinux'},
    {'fid':'fosinopril_dose', 'pos':'fosinopril'},
    {'fid':'moexipril_dose', 'pos':'moexipril'},
    {'fid':'perindopril_dose', 'pos':'perindopril'},
    {'fid':'quinapril_dose', 'pos':'quinapril'},
    {'fid':'trandolapril_dose', 'pos':'trandolapril'},
    {'fid':'brivaracetam_dose', 'pos':'brivaracetam'},
    {'fid':'carbamazepine_dose', 'pos':'carbamazepine'},
    {'fid':'clobazam_dose', 'pos':'clobazam'},
    {'fid':'clonazepam_dose', 'pos':'clonazepam'},
    {'fid':'divalproex_dose', 'pos':'divalproex'},
    {'fid':'eslicarbazepine_dose', 'pos':'eslicarbazepine'},
    {'fid':'ethosuximide_dose', 'pos':'ethosuximide'},
    {'fid':'ethotoin_dose', 'pos':'ethotoin'},
    {'fid':'ezogabine_dose', 'pos':'ezogabine'},
    {'fid':'felbamate_dose', 'pos':'felbamate'},
    {'fid':'fosphenytoin_dose', 'pos':'fosphenytoin'},
    {'fid':'gabapentin_dose', 'pos':'gabapentin'},
    {'fid':'lacosamide_dose', 'pos':'lacosamide'},
    {'fid':'lamotrigine_dose', 'pos':'lamotrigine'},
    {'fid':'mephenytoin_dose', 'pos':'^mephenytoin'},
    {'fid':'mephobarbital_dose', 'pos':'mephobarbital'},
    {'fid':'methsuximide_dose', 'pos':'methsuximide'},
    {'fid':'oxcarbazepine_dose', 'pos':'oxcarbazepine'},
    {'fid':'paramethadione_dose', 'pos':'paramethadione'},
    {'fid':'perampanel_dose', 'pos':'perampanel'},
    {'fid':'phenacemide_dose', 'pos':'phenacemide'},
    {'fid':'pregabalin_dose', 'pos':'pregabalin'},
    {'fid':'primidone_dose', 'pos':'primidone'},
    {'fid':'rufinamide_dose', 'pos':'rufinamide'},
    {'fid':'tiagabine_dose', 'pos':'tiagabine'},
    {'fid':'trimethadione_dose', 'pos':'trimethadione'},
    {'fid':'valproate_sodium_dose', 'pos':'valproate sodium'},
    {'fid':'vigabatrin_dose', 'pos':'vigabatrin'},
    {'fid':'acarbose_dose', 'pos':'acarbose'},
    {'fid':'acetohexamide_dose', 'pos':'acetohexamide'},
    {'fid':'albiglutide_dose', 'pos':'albiglutide'},
    {'fid':'alogliptin_dose', 'pos':'alogliptin'},
    {'fid':'bromocriptine_dose', 'pos':'bromocriptine'},
    {'fid':'canagliflozin_dose', 'pos':'canagliflozin'},
    {'fid':'chlorpropamide_dose', 'pos':'chlorpropamide'},
    {'fid':'dapagliflozin_dose', 'pos':'dapagliflozin'},
    {'fid':'dulaglutide_dose', 'pos':'dulaglutide'},
    {'fid':'empagliflozin_dose', 'pos':'empagliflozin'},
    {'fid':'exenatide_dose', 'pos':'exenatide'},
    {'fid':'glimepiride_dose', 'pos':'glimepiride'},
    {'fid':'glipizide_dose', 'pos':'glipizide'},
    {'fid':'glyburide_dose', 'pos':'glyburide'},
    {'fid':'linagliptin_dose', 'pos':'linagliptin'},
    {'fid':'liraglutide_dose', 'pos':'liraglutide'},
    {'fid':'lixisenatide_dose', 'pos':'lixisenatide'},
    {'fid':'mifepristone_dose', 'pos':'mifepristone'},
    {'fid':'miglitol_dose', 'pos':'miglitol'},
    {'fid':'nateglinide_dose', 'pos':'nateglinide'},
    {'fid':'pioglitazone_dose', 'pos':'pioglitazone'},
    {'fid':'pramlintide_dose', 'pos':'pramlintide'},
    {'fid':'repaglinide_dose', 'pos':'repaglinide'},
    {'fid':'rosiglitazone_dose', 'pos':'rosiglitazone'},
    {'fid':'saxagliptin_dose', 'pos':'saxagliptin'},
    {'fid':'sitagliptin_dose', 'pos':'sitagliptin'},
    {'fid':'tolazamide_dose', 'pos':'tolazamide'},
    {'fid':'tolbutamide_dose', 'pos':'tolbutamide'},
    {'fid':'troglitazone_dose', 'pos':'troglitazone'},
    {'fid':'amiloride_dose', 'pos':'amiloride'},
    {'fid':'ammonium_chloride_dose', 'pos':'ammonium chloride'},
    {'fid':'bendroflumethiazide_dose', 'pos':'bendroflumethiazide'},
    {'fid':'chlorthalidone_dose', 'pos':'chlorthalidone'},
    {'fid':'conivaptan_dose', 'pos':'conivaptan'},
    {'fid':'dichlorphenamide_dose', 'pos':'dichlorphenamide'},
    {'fid':'eplerenone_dose', 'pos':'eplerenone'},
    {'fid':'ethacrynate_sodium_dose', 'pos':'ethacrynate sodium'},
    {'fid':'ethacrynic_acid_dose', 'pos':'ethacrynic acid'},
    {'fid':'hydroflumethiazide_dose', 'pos':'hydroflumethiazide'},
    {'fid':'methazolamide_dose', 'pos':'methazolamide'},
    {'fid':'methyclothiazide_dose', 'pos':'methyclothiazide'},
    {'fid':'metolazone_dose', 'pos':'metolazone'},
    {'fid':'pamabrom_dose', 'pos':'pamabrom'},
    {'fid':'polythiazide_dose', 'pos':'polythiazide'},
    {'fid':'tolvaptan_dose', 'pos':'tolvaptan'},
    {'fid':'torsemide_dose', 'pos':'torsemide'},
    {'fid':'trichlormethiazide_dose', 'pos':'trichlormethiazide'},
    {'fid':'flumazenil_dose', 'pos':'flumazenil'},
    {'fid':'nitroglycerin_dose', 'pos':'nitroglycerin'},
    {'fid':'nitroprusside_dose', 'pos':'nitroprusside'},
    {'fid':'phentolamine_dose', 'pos':'phentolamine'},
    {'fid':'digoxin_dose', 'pos':'LANOXIN'},
    {'fid':'magnesium_sulfate_dose', 'pos':'^magnesium sulfate'},
    {'fid':'alteplase_dose', 'pos':'alteplase'}
    # {
    #     'fid': 'fluids_intake',
    #     'pos': '^albumin human \(PLASBUMIN\)|^dextrose 5% lactated ringers bolus|^hetastarch|^lactated ringers|^sodium chloride 0.9( %|%) (?!injection|flush syringe|nebulizer)',
    # },
]
