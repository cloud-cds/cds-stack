from etl.transforms.primitives.df import derive
from etl.transforms.primitives.df import format_data
from etl.transforms.primitives.df import restructure
from etl.transforms.primitives.df import translate

import pandas as pd
from sqlalchemy import text

import etl.transforms.primitives.df.filter_rows as filter_rows


#============================
# Utilities
#============================
def pull_med_orders(engine_source):

    mo = pd.read_sql_query(text("""select "display_name", "MedUnit","Dose" from "OrderMed";"""),con=engine_source)

    mo = restructure.select_columns(mo, {'display_name': 'full_name',
                                        'MedUnit':'dose_unit',
                                        'Dose': 'dose'})

    mo = mo.dropna(subset=['full_name'])
    mo = translate.translate_med_name_to_fid(mo)
    mo = filter_rows.filter_medications(mo)
    mo = format_data.clean_units(mo, 'fid', 'dose_unit')
    mo = format_data.clean_values(mo, 'fid', 'dose')
    mo = translate.convert_units(mo, fid_col='fid',
                                 fids=['piperacillin_tazbac_dose', 'vancomycin_dose',
                                          'cefazolin_dose', 'cefepime_dose', 'ceftriaxone_dose',
                                          'ampicillin_dose'],
                                 unit_col='dose_unit', from_unit='g', to_unit='mg',
                                 value_col='dose', convert_func=translate.g_to_mg)


    mo = derive.combine(mo, 'vasopressors_dose',
                        ['vasopressin_dose', 'neosynephrine_dose', 'levophed_infusion_dose',
                               'lactated_ringers', 'epinephrine_dose', 'dopamine_dose',
                               'dobutamine_dose'])

    mo = derive.combine(mo, 'crystalloid_fluid',
                        ['lactated_ringers', 'sodium_chloride'])

    mo = derive.combine(mo, 'cms_antibiotics',
                        ['cefepime_dose', 'ceftriaxone_dose', 'piperacillin_tazbac_dose',
                               'levofloxacin_dose', 'moxifloxacin_dose', 'vancomycin_dose',
                               'metronidazole_dose', 'aztronam_dose', 'ciprofloxacin_dose',
                               'gentamicin_dose', 'azithromycin_dose', ])

    mo = format_data.threshold_values(mo, 'dose')

    mo['fid'] += '_order'

    return mo

def pull_medication_admin(engine_source):

    ma = pd.read_sql_query(text("""select CSN_ID, display_name,
                                          Dose, MedUnit,
                                          INFUSION_RATE, MAR_INF_RATE_UNIT,
                                          TimeActionTaken
                                          from "OrderMed";"""),con=engine_source)

    ma = restructure.select_columns(ma, {'CSN_ID': 'csn_id',
                                        'display_name':'full_name',
                                        'Dose':'dose_value',
                                        'MedUnit':'dose_unit',
                                        'INFUSION_RATE':'rate_value',
                                        'MAR_INF_RATE_UNIT':'rate_unit',
                                        'TimeActionTaken':'tsp'})

    ma = translate.translate_med_name_to_fid(ma)
    ma = filter_rows.filter_medications(ma)
    ma = translate.convert_units(ma,
                                 fid_col = 'fid',
                                 fids = ['piperacillin_tazbac_dose', 'vancomycin_dose',
                            'cefazolin_dose', 'cefepime_dose', 'ceftriaxone_dose',
                            'ampicillin_dose'],
                                 unit_col = 'dose_unit', from_unit = 'g', to_unit = 'mg',
                                 value_col = 'dose_value', convert_func = translate.g_to_mg)

    ma = derive.combine(ma, 'fluids_intake',
                        ['albumin_dose', 'hetastarch', 'sodium_chloride',
                        'lactated_ringers'])

    ma = derive.combine(ma, 'vasopressors_dose',
                        ['vasopressin_dose', 'neosynephrine_dose', 'levophed_infusion_dose',
                        'epinephrine_dose', 'dopamine_dose', 'milrinone_dose'
                                                             'dobutamine_dose'])
    ma = derive.combine(ma, 'crystalloid_fluid',
                        ['lactated_ringers', 'sodium_chloride'])

    ma = derive.combine(ma, 'cms_antibiotics',
                        ['cefepime_dose', 'ceftriaxone_dose', 'piperacillin_tazbac_dose',
                        'levofloxacin_dose', 'moxifloxacin_dose', 'vancomycin_dose',
                        'metronidazole_dose', 'aztronam_dose', 'ciprofloxacin_dose',
                        'gentamicin_dose', 'azithromycin_dose', ])

    ma = format_data.threshold_values(ma, 'dose_value')
    return ma

#============================
# Extract functions
#============================

def bands_t(engine_source, engine_destination):

    labs = pd.read_sql_query(text("""SELECT "CSN_ID", "NAME" ,
                                  "ResultValue", "RESULT_TIME", "REFERENCE_UNIT"
                                  FROM "Labs_643";"""),con=engine_source)

    labs = restructure.select_columns(labs, {'CSN_ID': 'csn_id',
                                             'NAME': 'fid',
                                             'ResultValue': 'value',
                                             'RESULT_TIME': 'tsp',
                                             'REFERENCE_UNIT': 'unit'})

    labs = derive.combine(labs, 'lactate',
                          ['LACTATE, WHOLE BLOOD', 'LACTATE, PLASMA'])

    print("Write Complete")

def crystalloid_fluid_order_t(engine_source, engine_destination):
    pass
