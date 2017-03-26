import etl.transforms.confidence as confidence

def convert_gender_to_int(entry, log):
    value = entry[-1]
    if value == "Female":
        return [0, confidence.VALUE_TRANSFORMED]
    else:
        return [1, confidence.VALUE_TRANSFORMED]