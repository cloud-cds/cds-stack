"""
This diagnosis ICD9 code mapping is created from feature_mapping.csv in hcgh_1608 datalink
"""
DX_ICD9_MAPPING = [
    ('aids_diag', '^042'),
    ('chronic_bronchitis_diag', '^496'),
    ('chronic_kidney_diag', '^585\.'),
    ('diabetes_diag', '^250'),
    ('dialysis_diag', '^V45\.1'),
    ('esrd_diag', '^585\.6'),
    ('heart_arrhythmias_diag', '^427'),
    ('heart_failure_diag', '^428'),
    ('hem_malig_diag', '^200|^201|^202|^203|^204|^205|^206|^207|^208'),
    ('hypersensitivity_pneumonitis_diag', '^495.9'),
    ('immuno_comp_diag', '^V58\.65|^V58\.0|^V58\.1|^042|^208\.0|^202\.'),
    ('immunodeficiency_diag', '^279\.3'),
    ('liver_disease_diag', '^571'),
    ('met_carcinoma_diag', '^140|^141|^142|^143|^144|^145|^146|^147|^148|^149|^150|^151|^152|^153|^154|^155|^156|^157|^158|^159|^160|^161|^162|^163|^164|^165|^166|^167|^168|^169|^170|^171|^172|^173|^174|^175|^179|^180|^181|^182|^183|^184|^185|^186|^187|^188|^189|^190|^191|^192|^193|^194|^195|^196|^197|^198|^199'),
    ('organ_insufficiency_diag', '^571|^585\.6|^428\.22|^428\.32|^428\.42|^518\.83'),
    ('pancreatitis_chronic_diag', '^V58\.65|^V58\.0|^V58\.1|^042|^208\.0|^202\.'),
    ('renal_insufficiency_diag', '^585\.9'),
]

HX_ICD9_MAPPING = [
    ('aids_hist', '^042'),
    ('chronic_bronchitis_hist', '^49(0|1)'),
    ('chronic_kidney_hist', '^585\.'),
    ('chronic_pulmonary_hist', '^491|^496'),
    ('diabetes_hist', '^250'),
    ('dialysis_hist', '^V45\.1'),
    ('emphysema_hist', '^492'),
    ('esrd_hist', '^585\.6'),
    ('heart_failure_hist', '^428'),
    ('hypersensitivity_pneumonitis_hist', '^495\.9'),
    ('immunodeficiency_hist', '^279.3'),
    ('liver_disease_hist', '^571'),
    ('met_carcinoma_hist', '^140|^141|^142|^143|^144|^145|^146|^147|^148|^149|^150|^151|^152|^153|^154|^155|^156|^157|^158|^159|^160|^161|^162|^163|^164|^165|^166|^167|^168|^169|^170|^171|^172|^173|^174|^175|^179|^180|^181|^182|^183|^184|^185|^186|^187|^188|^189|^190|^191|^192|^193|^194|^195|^196|^197|^198|^199'),
    ('organ_insufficiency_hist', '^571|^585\.6|^428\.22|^428\.32|^428\.42|^518\.83'),
    ('renal_insufficiency_hist', '^585\.9'),
]

PL_ICD9_MAPPING = [
    ('chronic_airway_obstruction_prob', '^496'),
    ('chronic_bronchitis_prob', '^496'),
    ('chronic_kidney_prob', '^585\.'),
    ('esrd_prob', '^585\.6'),
    ('gi_bleed_inhosp', '^(578\.9|792\.1)$'),
    ('heart_arrhythmias_prob', '^427'),
    ('hypersensitivity_pneumonitis_prob', '^495\.9'),
    ('immunodeficiency_prob', '^279.3'),
    ('stroke_inhosp', '^(277\.87|282\.61|336\.1|344\.9|368\.46|431|433\.01|433\.11|433\.21|434\.01|434\.11|434\.91|435\.9|436|437\.0|438\.0|438\.10|438\.11|438\.12|438\.13|438\.20|438\.21|438\.22|438\.6|438\.7|438\.82|438\.83|438\.84|438\.85|438\.89|438\.9|729\.2|780\.39|781\.2|781\.94|781\.99|784\.3|799\.59)$'),
]
