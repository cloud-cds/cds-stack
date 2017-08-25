import etl.mappings.confidence as confidence

def convert_proc_to_boolean(entry, log):
    start_time = entry['PROC_START_TIME']
    ending_time = entry['PROC_ENDING_TIME']
    if start_time:
        return [start_time, True, confidence.VALUE_TRANSFORMED]
    if ending_time:
        return [ending_time, False, confidence.VALUE_TRANSFORMED]
