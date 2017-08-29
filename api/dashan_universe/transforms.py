from dashan_universe.med_regex import med_regex
import re


def transform_lab_orders(lab_orders):
    order_ids = {'blood_culture': ['7564'], 'lactate': ['1022', '838', '64630']}
    order_tsps = {'blood_culture': [], 'lactate': []}

    for order in lab_orders:
        if order.get('OrderStatus') in ['Signed', '']:
            if order.get('ProcedureID') in order_ids['blood_culture']:
                order_tsps['blood_culture'].append(order.get('OrderInstant'))
            elif order.get('ProcedureID') in order_ids['lactate']:
                order_tsps['lactate'].append(order.get('OrderInstant'))

    return order_tsps



def transform_med_orders(med_orders):
    good_meds = str("|").join(med['pos'] for med in med_regex)

    def find_order_from_name(med_name):
        if not re.search(good_meds, med_name, flags=re.I):
            return ''
        for med in med_regex:
            if re.search(med['pos'], med_name, flags=re.I):
                if 'neg' in med and len(med['neg']) > 0 and re.search(med['neg'], med_name, flags=re.I):
                    return ''
                for part_of in med.get('part_of', []):
                    if part_of in ['cms_antibiotics', 'vasopressors_dose', 'crystalloid_fluid']:
                        return part_of
                return ''
        raise ValueError('Error in medication regex search for: {}'.format(med_name))

    order_tsps = {'antibiotics': [], 'crystalloid_fluid': [], 'vasopressors': []}

    for order in med_orders.get('MedicationOrders', []):
        order_type = find_order_from_name(order.get('Name', ''))
        if order_type == 'cms_antibiotics':
            order_tsps['antibiotics'].append(order.get('OrderInstant'))
        elif order_type == 'vasopressors_dose':
            order_tsps['vasopressors'].append(order.get('OrderInstant'))
        elif order_type == 'crystalloid_fluid':
            order_tsps['crystalloid_fluid'].append(order.get('OrderInstant'))

    return order_tsps
