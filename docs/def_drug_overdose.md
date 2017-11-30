# Define suspicion of drug overdose group
## This is what we'd use to define subpopulation

Suspicion of drug overdose if any of the following fids **ordered** in cdm_t:
* nalaxone_dose
* flumazenil_dose
* \*_screen
* cocaine
* methadone

`select enc_id, min(tsp) from cdm_t where fid ~'nalaxone|flumazenil|screen|cocaine|methadone;`

# Retrospective definition of drug overdose
1. Administration of nalaxone_dose or flumazenil_dose followed by increase in GCS
⋅⋅* Define increase in GCS as increase of X points, maybe 2?
⋅⋅* This might be problematic if drug is given first in field. Could only require increase in GCS if GCS prior to administration is < 6
2. Abnormal result from among (\*_screen, cocaine, methadone)
⋅⋅* We'd need to define what counts as abnormal. The value for this field is often text or a value like '<0.05'
