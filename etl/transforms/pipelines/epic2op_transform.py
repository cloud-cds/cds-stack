import etl.transforms.primitives.df.derive as derive
import etl.transforms.primitives.df.filter_rows as filter_rows
import etl.transforms.primitives.df.format_data as format_data
import etl.transforms.primitives.df.restructure as restructure
import etl.transforms.primitives.df.translate as translate
from etl.mappings.flowsheet_ids import flowsheet_ids
from etl.mappings.active_procedure_ids import active_procedure_ids
from etl.mappings.active_procedure_names import active_procedure_names
from etl.mappings.component_ids import component_ids
from etl.mappings.med_regex import med_regex

bedded_patients_transforms = [
    lambda bp: restructure.select_columns(bp, {
        'PatientIDs':       'pat_id',
        'AdmitDateTime':    'admittime',
        'PatientClass':     'patient_class',
        'CSN':              'visit_id',
        'Age':              'age',
        'Gender':           'gender',
        'AdmitDx':          'diagnosis',
        'MedicalHistory':   'history',
        'ProblemList':      'problem_all',
        'HospProblemList':  'problem',
        'hospital':         'hospital',
    }),
    lambda bp: restructure.extract_id_from_list(bp, 'pat_id', 'EMRN'),
    lambda bp: filter_rows.filter_on_icd9(bp),
    lambda bp: filter_rows.filter_invalid_patient_classes(bp, 'patient_class'),
    lambda bp: format_data.format_numeric(bp, 'age'),
    lambda bp: format_data.format_gender_to_string(bp, 'gender'),
]

ed_patients_transforms = [
    lambda bp: restructure.select_columns(bp, {
        'PatientIDs':       'pat_id',
        'CSN':              'visit_id',
        'hospital':         'hospital',
    }),
    lambda bp: restructure.extract_id_from_list(bp, 'pat_id', 'EMRN')
]

chiefcomplaint_transforms = [
    lambda cc: restructure.select_columns(cc, {
        'pat_id':   'pat_id',
        'visit_id': 'visit_id',
        'Items':    'value'
    }),
]

treatmentteam_transforms = [
    lambda tt: restructure.select_columns(tt, {
        'pat_id': 'pat_id',
        'visit_id': 'visit_id',
        'TeamMembers': 'value'
        }),
    lambda tt: format_data.format_treatmentteam(tt)
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
    lambda fs: translate.convert_weight_value_to_float(fs, 'fid', 'value', 'weight'),
    lambda fs: format_data.clean_values(fs, 'fid', 'value'),
    lambda fs: translate.extract_sys_dias_from_bp(fs, 'fid', 'value', 'nbp'),
    lambda fs: translate.extract_sys_dias_from_bp(fs, 'fid', 'value', 'abp'),
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
    lambda fs: translate.convert_units(fs,
        fid_col = 'fid', fids = ['dialysis'],
        unit_col = 'unit', from_unit = '', to_unit = '',
        value_col = 'value', convert_func = translate.ml_to_boolean
    ),
    lambda fs: format_data.filter_to_final_units(fs, 'unit'),
    lambda fs: format_data.threshold_values(fs, 'value'),
    lambda fs: translate.convert_to_boolean(fs, 'fid', 'value', 'dialysis')
    # lambda fs: derive.sum_values_at_same_tsp(fs,
    #     ['urine_output', 'fluids_intake']
    # ),
]

active_procedures_transforms = [
    lambda lo: restructure.select_columns(lo, {
        'pat_id':                   'pat_id',
        'visit_id':                 'visit_id',
        'OrderInstant':             'tsp',
        'ProcedureId':              'procedure_id',
        'OrderStatus':              'order_status',
        'ProcedureResultStatus':    'proc_status',
        'OrderId':                  'order_id',
        'ProcedureName':            'procedure_name',
    }),
    lambda lo: translate.translate_epic_id_to_fid(lo,
        col = 'procedure_id', new_col = 'fid',
        config_map = active_procedure_ids, drop_original = True,
        add_string = '_order', add_string_fid=['blood_culture', 'lactate', 'culture'], remove_if_not_found = True,
        name_col= 'procedure_name', name_config_map=active_procedure_names
    ),
    lambda lo: derive.derive_procedure_status(lo),
]

lab_orders_transforms = [
    lambda lp: restructure.select_columns(lp, {
        'pat_id':           'pat_id',
        'visit_id':         'visit_id',
        'Components':       'components',
        'CollectionDate':   'date',
        'CollectionTime':   'time',
        'ResultDate':       'res_date',
        'ResultTime':       'res_time',
    }),
    lambda lp: restructure.unlist(lp, 'components'),
    lambda lp: restructure.extract(lp, 'components', {
        'OrderID':          'order_id',
        'Status':           'status',
        'ComponentID':      'component_id',
    }),
    lambda lp: restructure.extract(lp, 'status', {'Title': 'status'}),
    lambda lp: restructure.make_null_time_midnight(lp), # TEMPORARY WORKAROUND
    lambda lp: restructure.concat_str(lp, 'tsp', 'date', 'time'),
    lambda lp: restructure.concat_str(lp, 'res_tsp', 'res_date', 'res_time'),
    lambda lp: translate.translate_epic_id_to_fid(lp,
        col = 'component_id', new_col = 'fid',
        config_map = component_ids, drop_original = True,
        add_string = '_order', add_string_fid=['blood_culture', 'lactate'],
        remove_if_not_found = True
    ),
    lambda lp: format_data.format_tsp(lp, 'tsp'),
    lambda lp: format_data.format_tsp(lp, 'res_tsp'),
    lambda lp: derive.use_correct_tsp(lp, first='tsp', second='res_tsp'),
]

lab_results_transforms = [
    lambda lr: restructure.select_columns(lr, {
        'pat_id':           'pat_id',
        'visit_id':         'visit_id',
        'ComponentID':      'component_id',
        'CollectionDate':   'date',
        'CollectionTime':   'time',
        'ResultDate':       'res_date',
        'ResultTime':       'res_time',
        'Value':            'value',
        'Units':            'unit',
    }),
    lambda lr: restructure.unlist(lr, 'value'),
    lambda lr: restructure.make_null_time_midnight(lr), # TEMPORARY WORKAROUND
    lambda lr: restructure.concat_str(lr, 'tsp', 'date', 'time'),
    lambda lr: restructure.concat_str(lr, 'res_tsp', 'res_date', 'res_time'),
    lambda lr: translate.translate_epic_id_to_fid(lr,
        col = 'component_id', new_col = 'fid',
        config_map = component_ids,
        drop_original = True,
    ),
    lambda lr: format_data.filter_empty_values(lr, 'tsp'),
    lambda lr: format_data.clean_units(lr, 'fid', 'unit'),
    lambda lr: format_data.clean_values(lr, 'fid', 'value'),
    lambda lr: format_data.threshold_values(lr, 'value'),
    lambda lr: translate.convert_units(lr,
        fid_col = 'fid',
        fids = ['ddimer'],
        unit_col = 'unit', from_unit = 'mg/L', to_unit = 'mg/L FEU',
        value_col = 'value',
        convert_func = translate.mg_per_l_to_mg_per_l_feu
    ),
    lambda lr: format_data.filter_to_final_units(lr, 'unit'),
    lambda lr: format_data.format_tsp(lr, 'tsp'),
    lambda lr: format_data.format_tsp(lr, 'res_tsp'),
    lambda lr: derive.use_correct_tsp(lr, first='tsp', second='res_tsp'),
]

C_fids = [ med['fid'] for med in med_regex if 'part_of' in med and 'C' in med['part_of']]

F_fids = [ med['fid'] for med in med_regex if 'part_of' in med and 'F' in med['part_of']]

V_fids = [ med['fid'] for med in med_regex if 'part_of' in med and 'V_dose' in med['part_of']]


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
        'DiscontinueInstant':   'discontinue_tsp',
        'EndDateTime':          'end_tsp',
        'OrderMode':            'order_mode'
    }),
    lambda mo: restructure.extract(mo, 'dose_unit', {'Title': 'dose_unit'}),
    lambda mo: restructure.extract(mo, 'frequency', {'Name': 'frequency'}),
    lambda mo: translate.translate_med_name_to_fid(mo),
    # lambda mo: filter_rows.filter_medications(mo),
    # lambda mo: format_data.clean_units(mo, 'fid', 'dose_unit'),
    lambda mo: format_data.to_numeric(mo, 'fid', 'dose', default_value=99),
    lambda mo: translate.convert_units(mo,
        fid_col = 'fid',
        fids = ['A_dose', 'B_dose', 'C_dose'],
        # medications require unit convertion from g to mg
        unit_col = 'dose_unit', from_unit = 'g', to_unit = 'mg',
        value_col = 'dose', convert_func = translate.g_to_mg
    ),
    # lambda mo: derive.combine(mo, 'fluids_intake', fluids_intake_fids),
    lambda mo: derive.combine(mo, 'V_dose', V_fids),
    lambda mo: derive.combine(mo, 'F', F_fids),
    lambda mo: derive.combine(mo, 'C', C_fids),
    lambda mo: format_data.add_order_to_fid(mo),
    # lambda mo: format_data.threshold_values(mo, 'dose'),
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
    # lambda ma: filter_rows.filter_medications(ma),
    #lambda ma: filter_rows.filter_stopped_medications(ma),
    #lambda ma: translate.override_empty_doses_with_rates(ma, fid_col='fid', fids=['sodium_bicarbonate']),
    #lambda ma: filter_rows.filter_by_doses(ma),
    lambda ma: translate.convert_units(ma,
        fid_col = 'fid',
        fids = ['A_dose', 'B_dose', 'C_dose'],
        unit_col = 'dose_unit', from_unit = 'g', to_unit = 'mg',
        value_col = 'dose_value', convert_func = translate.g_to_mg
    ),
    lambda ma: translate.convert_units(ma,
       fid_col = 'fid',
       fids = F_fids,
       unit_col = 'dose_unit', from_unit = 'mL/hr', to_unit = 'mL',
       value_col = 'dose_value', convert_func = translate.ml_per_hr_to_ml_for_1hr
    ),
    # lambda ma: derive.combine(ma, 'fluids_intake', fluids_intake_fids),
    lambda ma: derive.combine(ma, 'V_dose', V_fids),
    lambda ma: derive.combine(ma, 'F', F_fids),
    lambda ma: derive.combine(ma, 'C', C_fids),
    # lambda ma: format_data.threshold_values(ma, 'dose_value')
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

notes_transforms = [
    lambda n: restructure.select_columns(n, {
        'CSN'       : 'visit_id',
        'pat_id'    : 'pat_id',
        'Key'       : 'note_id',
        'NoteType'  : 'note_type',
        'Status'    : 'note_status',
        'Dates'     : 'dates',
        'Providers' : 'providers',
    }),
    lambda n: format_data.json_encode(n, 'dates'),
    lambda n: format_data.json_encode(n, 'providers'),
]

note_texts_transforms = [
    lambda nt: restructure.select_columns(nt, {
        'Key'          : 'note_id',
        'DocumentText' : 'note_body',
    }),
    lambda nt: format_data.base64_safe_decode(nt, 'note_body'),
]

contacts_transforms = [
    lambda c: restructure.select_columns(c, {
        'CSN'           : 'visit_id',
        'enc_id'        : 'enc_id',
        'ContactDate'   : 'encounter_date',
        'DischargeDate' : 'discharge_date',
        'DischargeDisposition': 'discharge_disposition',
        'DepartmentName': 'department',
        'EncounterType' : 'encounter_type',
    }),
    lambda c: format_data.format_tsp(c, 'discharge_date', no_NaT=True),
]
