import numpy as np
from dashan_ml.input import DashanInput

def main():
    lambda_list = np.loadtxt('scripts/hcgh_test.lambda_list.txt', delimiter=',') #this list should not be changed
    lambda_list = lambda_list[5:]

    # featureOrig_list = np.loadtxt('sepsis_features.csv', delimiter=',', dtype=str,usecols=(0,))
    # featureOrig_list = list(featureOrig_list)
    #
    # conditionalFeatures_list = np.loadtxt('sepsis_features_ckd_gender.csv', delimiter=',', dtype=str,usecols=(0,))
    # conditionalFeatures_list = list(conditionalFeatures_list)

    impFeatsList = ['pao2'
                    , 'hepatic_sofa'
                    , 'paco2'
                    , 'sodium'
                    , 'rass'
                    , 'sirs_raw'
                    , 'pao2_to_fio2'
                    , 'fio2'
                    , 'neurologic_sofa'
                    , 'hematologic_sofa'
                    , 'renal_sofa'
                    , 'nbp_sys'
                    , 'sirs_hr_oor'
                    , 'resp_sofa'
                    , 'bun_to_cr'
                    , 'cardio_sofa'
                    , 'wbc'
                    , 'shock_idx'
                    , 'weight'
                    , 'platelets'
                    , 'arterial_ph'
                    , 'nbp_dias'
                    , 'co2'
                    , 'ast_liver_enzymes'
                    , 'fluids_intake_24hr'
                    , 'ptt'
                    , 'lipase'
                    , 'hypotension_raw'
                    , 'sbpm'
                    , 'heart_rate'
                    , 'nbp_mean'
                    , 'urine_output_24hr'
                    , 'amylase'
                    , 'temperature'
                    , 'sirs_wbc_oor'
                    , 'urine_output_6hr'
                    , 'spo2'
                    , 'resp_rate'
                    , 'bun'
                    , 'hypotension_intp'
                    , 'worst_sofa'
                    , 'hemoglobin'
                    , 'any_organ_failure'
                    , 'inr'
                    , 'creatinine'
                    , 'bilirubin'
                    , 'mapm'
                    , 'gcs'
                    , 'sirs_temperature_oor'
                    , 'sirs_resp_oor'
                    , 'lactate'
                    , 'minutes_since_any_organ_fail'
                    , 'heart_arrhythmias_diag'
                    , 'chronic_bronchitis_diag'
                    , 'esrd_prob'
                    , 'renal_insufficiency_diag'
                    , 'heart_failure_hist'
                    , 'diabetes_diag'
                    , 'gender'
                    , 'esrd_diag'
                    , 'liver_disease_hist'
                    , 'emphysema_hist'
                    , 'esrd_hist'
                    , 'liver_disease_diag'
                    , 'organ_insufficiency_hist'
                    , 'age'
                    , 'heart_arrhythmias_prob'
                    , 'met_carcinoma_diag'
                    , 'heart_failure_diag'
                    , 'chronic_bronchitis_prob'
                    , 'hem_malig_diag'
                    , 'immuno_comp_diag'
                    , 'met_carcinoma_hist'
                    , 'chronic_pulmonary_hist'
                    , 'chronic_bronchitis_hist'
                    , 'pancreatitis_chronic_diag'
                    , 'chronic_airway_obstruction_prob']

    featureConstraints = {'lactate': np.array([0,np.inf])}

    out=DashanInput.inputParams(data_id='lactateConstr',adverse_event='severe_sepsis',
                                lambda_list=lambda_list, feature_list=impFeatsList,featureConstraints=featureConstraints,
                                numberOfIterations=50, ncpus=50,
                                evaluationLambdas=[0.008, 0.005, 0.003, 0.001, 0.0008, 0.0005, 0.0003],
                                sensitivityTargets=[0.15, 0.50, 0.85],
                                forceRedo=True).toPickle()
    print "Complete"

if __name__ == "__main__":
    main()

# subtypes = {
#     'heart_failure': ['heart_failure_diag', 'heart_failure_hist'],
#     'heapatic_failure': ['hepatic_failure_inhosp', 'acute_liver_failure'],
#     'chronic_kidney_disease': ['chronic_kidney_hist', 'esrd_hist', 'esrd_diag', 'esrd_prob', 'renal_insufficiency_hist'],
#     'met_carcinoma': ['met_carcinoma_hist', 'met_carcinoma_diag'],
#     'immuno_comp': ['immuno_comp_hist', 'immuno_comp_diag'],
#     'asthma': ['asthma_hist', 'asthma_diag', 'asthma_prob'],
#     'bronchiectasis': ['bronchiectasis_hist', 'bronchiectasis_diag', 'bronchiectasis_prob'],
#     'chronic_airway_obstruction': ['chronic_airway_obstruction_hist', 'chronic_airway_obstruction_diag', 'chronic_airway_obstruction_prob'],
#     'chronic_bronchitis': ['chronic_bronchitis_hist', 'chronic_bronchitis_diag', 'chronic_bronchitis_prob'],
#     'emphysema': ['emphysema_hist', 'emphysema_diag', 'emphysema_prob'],
#     'hypersensitivity_pneumonitis': ['hypersensitivity_pneumonitis_hist',' hypersensitivity_pneumonitis_diag', 'hypersensitivity_pneumonitis_prob']}