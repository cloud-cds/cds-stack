import sys
# sys.path.remove('/home/ubuntu/zad/dashan-db/ml')
print(sys.path)
import numpy as np
from ml.dashan_input import InputParams
from ml.constants import Lambda_List
import os

def main():

    lambda_list = Lambda_List[5:]

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

    print("Writing Input")

    this_file = InputParams(
                                name='train',
                                dataset_id=1, #int(os.environ['dataset_id']),
                                adverse_event='severe_sepsis',
                                lambda_list=lambda_list, feature_list=impFeatsList,featureConstraints=featureConstraints,
                                numberOfIterations=4, ncpus=8,
                                evaluationLambdas=[0.008, 0.005, 0.003, 0.001],
                                sensitivityTargets=[0.85, 0.50, 0.15],
                                forceRedo=False,
                                downSampleFactor=50,
                                maxNumRows=100000,
                                )
    this_file.to_pickle_file()

    print("Complete")

if __name__ == "__main__":
    main()
