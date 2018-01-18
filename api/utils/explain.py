from collections import defaultdict

def thresholdImportances(importances):
    """
    Args:
        importances: a dictionary of the form {feature:importance}
    Returns:
        top_importances: a dictionary of thresholded importances of the form {feature:importance}
    """
    top_entries = sorted(importances.items(), key=lambda entry: entry[1], reverse=True)[:5]
    top_entries = filter(lambda entry: entry[1]>0, top_entries)
    result = {}
    for (k,v) in top_entries:
      result[k] = v
    return result
   
    """
    results = {}
    entries = list(importances.iteritems())
    maximum = max(entries,key=lambda item:item[1])[1]
    thresholded_entries = filter(lambda (key,val): val>.1*maximum,entries)
    for key,val in thresholded_entries:
       results[key] = val
    return results
    """
def getMappedImportances(importances, mapping):
    """
    Args:
        importances: dictionary of importances of the form {feature:importance}
        mapping: a dictionary mapping from features to measurement of the form {feature:measurement}
    Returns:
        mapped_importances: a dictionary of measurement importances
    """
    mapped_importances = defaultdict(float)
    for feature,importance in importances.items():
        try:
            for measurement in mapping[feature]:
                mapped_importances[measurement] += importance
        except KeyError:
            mapped_importances[feature] = importance
    return mapped_importances
    
    
    
    
