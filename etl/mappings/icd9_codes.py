"""
This diagnosis ICD9 code mapping is created from feature_mapping.csv in hcgh_1608 datalink
"""
DX_ICD9_MAPPING = [
    ('diabetes_diag', '^250'),
    ('esrd_diag', '^585.6'),
    ('renal_insufficiency_diag', '^585.9'),
    ('heart_arrhythmias_diag', '^427'),
    ('heart_failure_diag', '^428'),
    ('liver_disease_diag', '^571'),
    ('chronic_bronchitis_diag', '^496'),
    ('met_carcinoma_diag', '^140|^141|^142|^143|^144|^145|^146|^147|^148|^149|^150|^151|^152|^153|^154|^155|^156|^157|^158|^159|^160|^161|^162|^163|^164|^165|^166|^167|^168|^169|^170|^171|^172|^173|^174|^175|^179|^180|^181|^182|^183|^184|^185|^186|^187|^188|^189|^190|^191|^192|^193|^194|^195|^196|^197|^198|^199'),
    ('heart_failure_diag', '^428'),
    ('hem_malig_diag', '^200|^201|^202|^203|^204|^205|^206|^207|^208'),
    ('immuno_comp_diag', '^V58.65|^V58.0|^V58.1|^042|^208.0|^202.'),
    ('pancreatitis_chronic_diag', '^V58.65|^V58.0|^V58.1|^042|^208.0|^202.'),
]

HX_ICD9_MAPPING = [
    ('chronic_pulmonary_hist', '^491|^496'),
    ('esrd_hist', '^585.6'),
    ('organ_insufficiency_hist', '^571|^585.6|^428.22|^428.32|^428.42|^518.83'),
    ('heart_failure_hist', '^428'),
    ('liver_disease_hist', '^571'),
    ('emphysema_hist', '^492'),
    ('met_carcinoma_hist', '^140|^141|^142|^143|^144|^145|^146|^147|^148|^149|^150|^151|^152|^153|^154|^155|^156|^157|^158|^159|^160|^161|^162|^163|^164|^165|^166|^167|^168|^169|^170|^171|^172|^173|^174|^175|^179|^180|^181|^182|^183|^184|^185|^186|^187|^188|^189|^190|^191|^192|^193|^194|^195|^196|^197|^198|^199%'),
    ('chronic_bronchitis_hist', '^49(0|1)'),
]

PL_ICD9_MAPPING = [
    ('chronic_bronchitis_prob', '^496'),
    ('heart_arrhythmias_prob', '^427'),
    ('chronic_airway_obstruction_prob', '^496'),
    ('esrd_prob', '^585.6'),
]
