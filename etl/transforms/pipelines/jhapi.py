import etl.transforms.primitives.df.derive as derive
import etl.transforms.primitives.df.filter_rows as filter_rows
import etl.transforms.primitives.df.format_data as format_data
import etl.transforms.primitives.df.restructure as restructure
import etl.transforms.primitives.df.translate as translate
from etl.mappings.flowsheet_ids import flowsheet_ids
from etl.mappings.procedure_ids import procedure_ids
from etl.mappings.component_ids import component_ids

bedded_patients_transforms = [
    lambda bp: restructure.select_columns(bp, {
        'PatientIDs':       'pat_id',
        'AdmitDateTime':    'admittime',
        'CSN':              'visit_id',
        'Age':              'age',
        'Gender':           'gender',
        'AdmitDx':          'diagnosis',
        'MedicalHistory':   'history',
        'ProblemList':      'problem_all',
        'HospProblemList':  'problem',
    }),
    lambda bp: restructure.extract_id_from_list(bp, 'pat_id', 'EMRN'),
    lambda bp: filter_rows.filter_on_icd9(bp),
    lambda bp: format_data.format_numeric(bp, 'age'),
    lambda bp: format_data.format_gender_to_int(bp, 'gender'),
]

flowsheet_transforms = [
    lambda fs: restructure.select_columns(fs, {
        'pat_id':           'pat_id',
        'visit_id':         'visit_id',
        'FlowsheetRowID':   'flowsheet_id',
        'FlowsheetColumns': 'flowsheet_columns',
        'Unit':             'unit',
        'Name':             'name',
    }),
    lambda fs: restructure.extract_id_from_list(fs, 'flowsheet_id', 'INTERNAL'),
    lambda fs: restructure.unlist(fs, 'flowsheet_columns'),
    lambda fs: restructure.extract(fs, 'flowsheet_columns', {
        'Instant':          'tsp',
        'RawValue':         'value',
    }),
    lambda fs: translate.translate_epic_id_to_fid(fs,
        col = 'flowsheet_id', new_col = 'fid',
        config_map = flowsheet_ids,
        drop_original = True,
    ),
    lambda fs: format_data.clean_units(fs, 'fid', 'unit'),
    lambda fs: format_data.clean_values(fs, 'fid', 'value'),
    lambda fs: translate.extract_sys_dias_from_nbp(fs, 'fid', 'value'),
    lambda fs: translate.convert_units(fs,
        fid_col = 'fid', fids = ['temperature'],
        unit_col = 'unit', from_unit = 'Celcius', to_unit = 'Fahrenheit',
        value_col = 'value', convert_func = translate.celcius_to_fahrenheit
    ),
    lambda fs: translate.convert_units(fs,
        fid_col = 'fid', fids = ['rass'],
        unit_col = 'unit', from_unit = '', to_unit = '',
        value_col = 'value', convert_func = translate.rass_str_to_number
    ),
    lambda fs: format_data.filter_to_final_units(fs, 'unit'),
    lambda fs: format_data.threshold_values(fs, 'value'),
    lambda fs: derive.sum_values_at_same_tsp(fs,
        ['urine_output', 'fluids_intake']
    ),
]

lab_orders_transforms = [
    lambda lo: restructure.select_columns(lo, {
        'pat_id':                   'pat_id',
        'visit_id':                 'visit_id',
        'OrderInstant':             'tsp',
        'ProcedureCode':            'procedure_code',
        'OrderStatus':              'order_status',
        'ProcedureResultStatus':    'proc_status',
        'OrderId':                  'order_id',
    }),
    lambda lo: translate.translate_epic_id_to_fid(lo,
        col = 'procedure_code', new_col = 'fid',
        config_map = procedure_ids, drop_original = True,
        add_string = '_order', remove_if_not_found = True
    ),
    lambda lo: derive.derive_lab_status(lo),
]

lab_procedures_transforms = [
    lambda lp: restructure.select_columns(lp, {
        'pat_id':           'pat_id',
        'visit_id':         'visit_id',
        'Components':       'components',
        'ResultDate':       'date',
        'ResultTime':       'time',
    }),
    lambda lp: restructure.unlist(lp, 'components'),
    lambda lp: restructure.extract(lp, 'components', {
        'OrderID':          'order_id',
        'Status':           'status',
        'ComponentID':      'component_id',
    }),
    lambda lp: restructure.extract(lp, 'status', {'Title': 'status'}),
    lambda lp: restructure.concat_str(lp, 'tsp', 'date', 'time'),
    lambda lp: translate.translate_epic_id_to_fid(lp,
        col = 'component_id', new_col = 'fid',
        config_map = component_ids, drop_original = True,
        add_string = '_order'
    ),
    lambda lp: format_data.format_tsp(lp, 'tsp'),
]

lab_results_transforms = [
    lambda lr: restructure.select_columns(lr, {
        'pat_id':           'pat_id',
        'visit_id':         'visit_id',
        'ComponentID':      'component_id',
        'ResultDate':       'date',
        'ResultTime':       'time',
        'Value':            'value',
        'Units':            'unit',
    }),
    lambda lr: restructure.unlist(lr, 'value'),
    lambda lr: restructure.concat_str(lr, 'tsp', 'date', 'time'),
    lambda lr: translate.translate_epic_id_to_fid(lr,
        col = 'component_id', new_col = 'fid',
        config_map = component_ids,
        drop_original = True,
    ),
    lambda lr: format_data.filter_empty_values(lr, 'tsp'),
    lambda lr: format_data.clean_units(lr, 'fid', 'unit'),
    lambda lr: format_data.clean_values(lr, 'fid', 'value'),
    lambda lr: format_data.threshold_values(lr, 'value'),
    lambda lr: format_data.filter_to_final_units(lr, 'unit'),
    lambda lr: format_data.format_tsp(lr, 'tsp'),
]

med_orders_transforms = [
    lambda mo: restructure.select_columns(mo, {
        'pat_id':           'pat_id',
        'visit_id':         'visit_id',
        'MedicationOrders': 'orders',
    }),
    lambda mo: restructure.extract(mo, 'orders', {
        'OrderInstant':         'tsp',
        'Name':                 'full_name',
        'PatientFriendlyName':  'friendly_name',
        'OrderedDose':          'dose',
        'OrderedDoseUnit':      'dose_unit',
        'Frequency':            'frequency',
        'IDs':                  'ids',
    }),
    lambda mo: restructure.extract(mo, 'dose_unit', {'Title': 'dose_unit'}),
    lambda mo: restructure.extract(mo, 'frequency', {'Name': 'frequency'}),
    lambda mo: translate.translate_med_name_to_fid(mo),
    lambda mo: filter_rows.filter_medications(mo),
    lambda mo: format_data.clean_units(mo, 'fid', 'dose_unit'),
    lambda mo: format_data.clean_values(mo, 'fid', 'dose'),
    lambda mo: translate.convert_units(mo,
        fid_col = 'fid',
        fids = ['piperacillin_tazbac_dose', 'vancomycin_dose',
                'cefazolin_dose', 'cefepime_dose', 'ceftriaxone_dose',
                'ampicillin_dose'],
        unit_col = 'dose_unit', from_unit = 'g', to_unit = 'mg',
        value_col = 'dose', convert_func = translate.g_to_mg
    ),
    lambda mo: derive.combine(mo, 'fluids_intake',
        ['albumin_dose', 'hetastarch', 'sodium_chloride',
        'lactated_ringers']),
    lambda mo: derive.combine(mo, 'vasopressors_dose',
        ['vasopressin_dose', 'neosynephrine_dose', 'levophed_infusion_dose',
        'lactated_ringers', 'epinephrine_dose', 'dopamine_dose',
        'dobutamine_dose']),
    lambda mo: derive.combine(mo, 'crystalloid_fluid',
        ['lactated_ringers', 'sodium_chloride']),
    lambda mo: derive.combine(mo, 'cms_antibiotics',
        ['cefepime_dose', 'ceftriaxone_dose', 'piperacillin_tazbac_dose',
         'levofloxacin_dose', 'moxifloxacin_dose', 'vancomycin_dose',
         'metronidazole_dose', 'aztronam_dose', 'ciprofloxacin_dose',
         'gentamicin_dose', 'azithromycin_dose',]),
    lambda mo: format_data.threshold_values(mo, 'dose'),
]

med_admin_transforms = [
    lambda ma: restructure.select_columns(ma, {
        'pat_id':                       'pat_id',
        'visit_id':                     'visit_id',
        'MedicationAdministrations':    'admin',
        'Name':                         'full_name',
    }),
    lambda ma: restructure.unlist(ma, 'admin'),
    lambda ma: restructure.extract(ma, 'admin', {
        'Action':                   'action',
        'Dose':                     'dose',
        'Rate':                     'rate',
        'AdministrationInstant':    'tsp',
    }),
    lambda ma: restructure.extract(ma, 'dose', {
        'Value':    'dose_value',
        'Unit':     'dose_unit',
    }),
    lambda ma: restructure.extract(ma, 'rate', {
        'Value':    'rate_value',
        'Unit':     'rate_unit',
    }),
    lambda ma: translate.translate_med_name_to_fid(ma),
    lambda ma: filter_rows.filter_medications(ma),
    lambda ma: translate.convert_units(ma,
        fid_col = 'fid',
        fids = ['piperacillin_tazbac_dose', 'vancomycin_dose',
                'cefazolin_dose', 'cefepime_dose', 'ceftriaxone_dose',
                'ampicillin_dose'],
        unit_col = 'dose_unit', from_unit = 'g', to_unit = 'mg',
        value_col = 'dose_value', convert_func = translate.g_to_mg
    ),
    lambda ma: derive.combine(ma, 'fluids_intake',
        ['albumin_dose', 'hetastarch', 'sodium_chloride',
        'lactated_ringers']),
    lambda ma: derive.combine(ma, 'vasopressors_dose',
        ['vasopressin_dose', 'neosynephrine_dose', 'levophed_infusion_dose',
        'epinephrine_dose', 'dopamine_dose', 'milrinone_dose'
        'dobutamine_dose']),
    lambda ma: derive.combine(ma, 'crystalloid_fluid',
        ['lactated_ringers', 'sodium_chloride']),
    lambda ma: derive.combine(ma, 'cms_antibiotics',
        ['cefepime_dose', 'ceftriaxone_dose', 'piperacillin_tazbac_dose',
         'levofloxacin_dose', 'moxifloxacin_dose', 'vancomycin_dose',
         'metronidazole_dose', 'aztronam_dose', 'ciprofloxacin_dose',
         'gentamicin_dose', 'azithromycin_dose',]),
    lambda ma: format_data.threshold_values(ma, 'dose_value'),
    # lambda ma: derive.derive_fluids_intake(ma)
]

loc_history_transforms = [
    lambda x: filter_rows.filter_location_history_events(x),
    lambda x: restructure.select_columns(x, {
        'pat_id':            'pat_id',
        'EffectiveDateTime': 'tsp',
        'visit_id':          'visit_id',
        'UnitName':          'value',}),
    lambda x: x.assign(fid = 'care_unit'),
]
