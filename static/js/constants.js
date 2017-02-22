var INFECTIONS = [
"Endocarditis",
"Meningitis",
"Infections",
"Bacteremia",
"Cellulitis",
"UTI",
"Pneumonia",
"No Infection"
]

var ALERT_CODES = {
	"100": "TREWScore has passed the Septic Shock Risk Threshold",
	"101": "TREWScore has been elevated for ",
	"200": "All criteria for <b>Severe Sepsis</b> have been met",
	"201": "All criteria for <b>Septic Shock</b> have been met",
	"202": "<b>3hr</b> have passed since <b>Severe Sepsis</b> onset",
	"203": "<b>6hr</b> have passed since <b>Severe Sepsis</b> onset",
	"204": "<b>6hr</b> have passed since <b>Septic Shock</b> onset"
}

var EDIT = {
	"sirs": [
		"temperature",
		"heart rate",
		"respiratory rate",
		"wbc"
	],
	"org": [
		"blood pressure",
		"mean arterial pressure",
		"decrease in SBP",
		"respiratory failure",
		"creatinine",
		"bilirubin",
		"platelet count",
		"INR",
		"lactate"
	],
	"tension": [
		"SBP",
		"mean arterial pressure",
		"decrease in SBP"
	],
	"fusion": [
		"lactate"
	]
}

var CONSTANTS = {
	"sus-edit": "severe_sepsis",
	"sirs": "severe_sepsis",
	"org": "severe_sepsis",
	"tension": "septic_shock",
	"fusion": "septic_shock"
}

var severe_sepsis = {
	"display_name": "Severe Sepsis Criteria",
	"suspicion_of_infection": {
		"display_name": "Suspicion of Infection"
	},
	"sirs": {
		"display_name": "SIRS Criteria",
		"criteria": [{
			"key": "sirs_temp",
			"criteria_display_name": "Temperature is < 36.0 or > 38.3",
			"dropdown": "Temperature is normal",
			"overrideModal": [{
				"id": "override_temp",
				"header": "Override Temperature",
				"name": "Body Temperature",
				"units": "&#176;C",
				"step": 0.1,
				"range": "true",
				"minAbsolute": 20,
				"maxAbsolute": 50,
				"values": [36.0, 38.3]
			}]
		}, {
			"key": "heart_rate",
			"criteria_display_name": "Heart Rate (Pulse) > 90/min",
			"dropdown": "Heart Rate is normal",
			"overrideModal": [{
				"id": "override_heart_rate",
				"header": "Override Heart Rate",
				"name": "Heart Rate",
				"units": "/min",
				"step": 1,
				"range": "min",
				"minAbsolute": 20,
				"maxAbsolute": 240,
				"value": 90
			}]
		}, {
			"key": "respiratory_rate",
			"criteria_display_name": "Respiratory Rate > 20/min",
			"dropdown": "Respiratory rate is normal",
			"overrideModal": [{
				"id": "override_respiratory_rate",
				"header": "Override Respiratory Rate",
				"name": "Respiratory Rate",
				"units": "/min",
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 80,
				"value": 20
			}]
		}, {
			"key": "wbc",
			"criteria_display_name": "WBC < 4K/uL or > 12K/uL or >10% bands",
			"dropdown": "Wbc and/or Bands is normal",
			"overrideModal": [{
				"id": "override_wbc",
				"header": "Override White Blood Count",
				"name": "White Blood Count",
				"units": "K/uL",
				"step": 0.1,
				"range": "true",
				"minAbsolute": 2,
				"maxAbsolute": 15,
				"values": [4, 12]
			},{
				"id": "override_bands",
				"header": "Override Bands",
				"name": "Bands",
				"units": "%",
				"step": 0.1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 15,
				"value": 10
			}]
		}]
	},
	"organ_dysfunction": {
		"display_name": "Organ Dysfunction",
		"criteria": [{
			"key": "blood_pressure",
			"criteria_display_name": "Systolic Blood Pressue < 90",
			"dropdown": "Blood pressure is normal",
			"overrideModal": [{
				"id": "override_bp",
				"header": "Override Blood Pressure",
				"name": "Systolic Blood Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 90
			}]
		}, {
			"key": "mean_arterial_pressure",
			"criteria_display_name": "Mean arterial pressure < 65",
			"dropdown": "Mean arterial pressure is normal",
			"overrideModal": [{
				"id": "override_mean_arterial_pressure",
				"header": "Override Mean Arterial Pressure",
				"name": "Mean Arterial Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 65
			}]
		}, {
			"key": "decrease_in_sbp",
			"criteria_display_name": "Decrease in SBP by > 40 mmHg from the last recorded SBP considered normal for given patient",
			"dropdown": "Decrease in SBP is normal",
			"overrideModal": [{
				"id": "override_decrease_in_sbp",
				"header": "Override Decrease in Systolic Blood Pressure",
				"name": "Decrease in Systolic Blood Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 100,
				"value": 40
			}]
		}, {
			"key": "respiratory_failure",
			"criteria_display_name": "Acute respiratory failure evidenced by invasive or non-invasive ventiliation",
			"dropdown": "Respiratory rate is normal",
			"overrideModal": [{
				"id": "override_respiratory_rate",
				"header": "Override Respiratory Rate",
				"name": "Respiratory Failure",
				"units": "",
				"step": 1,
				"range": 'min',
				"minAbsolute": 0,
				"maxAbsolute": 200,
				"value": 100
			}]
		}, {
			"key": "creatinine",
			"criteria_display_name": "Creatinine > 2.0 or Urine Output < 0.5 mL/kg/hour for 2 hours",
			"dropdown": "Creatinine and/or Urine Output is normal",
			"overrideModal": [{
				"id": "override_creatinine",
				"header": "Override Creatinine",
				"name": "Creatinine",
				"units": "mg/dL",
				"step": 0.1,
				"range": "min",
				"minAbsolute": 0.0,
				"maxAbsolute": 4.0,
				"value": 2.0
			}, {
				"id": "override_urine_output",
				"header": "Override Urine Output",
				"name": "Urine Output",
				"units": "mL/kg/hour",
				"step": 0.1,
				"range": "max",
				"minAbsolute": 0.0,
				"maxAbsolute": 3.0,
				"value": 0.5
			}]
		}, {
			"key": "bilirubin",
			"criteria_display_name": "Bilirubin > 2 mg/dL (34.2 mmol/L)",
			"dropdown": "Bilirubin is normal",
			"overrideModal": [{
				"id": "override_bilirubin",
				"header": "Override Bilirubin",
				"name": "Bilirubin",
				"units": "mg/dL",
				"step": 0.01,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 4,
				"value": 2
			}]
		}, {
			"key": "platelet",
			"criteria_display_name": "Platelet count < 100,000",
			"dropdown": "Platelet Count is normal",
			"overrideModal": [{
				"id": "override_platelet_count",
				"header": "Override Platelet Count",
				"name": "Platelet Count",
				"units": "",
				"step": 100,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 200000,
				"value": 100000
			}]
		}, {
			"key": "inr",
			"criteria_display_name": "INR > 1.5 or PTT > 60 sec",
			"dropdown": "INR is normal",
			"overrideModal": [{
				"id": "override_inr",
				"header": "Override INR",
				"name": "INR",
				"units": "",
				"step": 1000,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 200000,
				"value": 100000
			},{
				"id": "override_ptt",
				"header": "Override PTT",
				"name": "PTT",
				"units": "sec",
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 120,
				"value": 60
			}]
		}, {
			"key": "lactate",
			"criteria_display_name": "Lactate > 2mmol/L: (18.0 mg/dL)",
			"dropdown": "Lactate is normal",
			"overrideModal": [{
				"id": "override_lactate_measurement",
				"header": "Override Lactate Measurement",
				"name": "Lactate Measurement",
				"units": "mmol/L",
				"step": 0.01,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 4,
				"value": 2
			}]
		}]
	}
}

var septic_shock = {
	"display_name": "Septic Shock Criteria",
	"tension": {
		"display_name": "Persistent Hypotension",
		"criteria": [{
			"key": "systolic_bp",
			"criteria_display_name": "Systolic blood pressure (SBP) < 90",
			"dropdown": "Systolic Blood Pressure is normal",
			"overrideModal": [{
				"id": "override_sbp",
				"header": "Override Systolic Blood Pressure",
				"name": "Systolic Blood Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 90
			}]
		}, {
			"key": "mean_arterial_pressure",
			"criteria_display_name": "Mean arterial pressue < 65",
			"dropdown": "Mean Arterial Pressure is normal",
			"overrideModal": [{
				"id": "override_mean_arterial_pressure",
				"header": "Override Mean Arterial Pressure",
				"name": "Mean Arterial Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 130,
				"value": 65
			}]
		}, {
			"key": "decrease_in_sbp",
			"criteria_display_name": "Decrease in SBP by > 40 mmHg from the last recorded SBP considered normal for given patient",
			"dropdown": "Decrease in sbp is normal",
			"overrideModal": [{
				"id": "override_decrease_in_sbp",
				"header": "Override Decrease in Systolic Blood Pressure",
				"name": "Decrease in Systolic Blood Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 80,
				"value": 40
			}]
		}]
	},
	"fusion": {
		"display_name": "Tissue Hypoperfusion",
		"criteria": [{
			"key": "initial_lactate",
			"criteria_display_name": "Initial Lactate level is >= 4 mmol/L",
			"dropdown": "Initial Lactate Measurement is normal",
			"overrideModal": [{
				"id": "override_initial_lactate_measurement",
				"header": "Override Initial Lactate Measurement",
				"name": "Initial Lactate Measurement",
				"units": "mmol/L",
				"step": 0.01,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 4,
				"value": 2
			}]
		}]
	}
}

workflows = {
	"sev3": {
		"display_name": "Severe Sepsis 3hr Workflow",
		"instruction": "Begins after onset of severe sepsis",
		"not_yet": "No severe sepsis, no action required."
	},
	"sev6": {
		"display_name": "Severe Sepsis 6hr Workflow",
		"instruction": "Begins after initial lactate measurement",
		"not_yet": "No severe sepsis, no action required."
	},
	"sep6": {
		"display_name": "Septic Shock 6hr Workflow",
		"instruction": "Begins after onset of septic shock",
		"not_yet": "No septic shock, no action required."
	},
	"init_lactate": {
		"display_name": "Initial Lactate"
	},
	"blood_culture": {
		"display_name": "Blood Culture"
	},
	"antibiotics": {
		"display_name": "Antibiotics"
	},
	"fluid": {
		"display_name": "Fluid"
	},
	"repeat_lactate": {
		"display_name": "Repeat Lactate"
	},
	"vasopressors": {
		"display_name": "Vasopressors"
	}
}

var STATIC = {
	"severe_sepsis": severe_sepsis,
	"septic_shock": septic_shock
}

var FID_TO_HUMAN_READABLE = {
  "vent": "Mechanical Ventilation",
  "pao2": "PaO2",
  "paco2": "PaCO2",
  "fio2": "FiO2",
  "amylase": "Amylase",
  "mapm": "MAP",
  "temperature": "Temperature",
  "sodium": "Sodium",
  "penicillin_dose": "Penicillin",
  "meropenem_dose": "Meropenem",
  "rass": "RASS",
  "resp_rate": "Respiratory Rate",
  "amoxicillin_dose": "Amoxicillin",
  "nbp": "Blood Pressure",
  "erythromycin_dose": "Erythromycin",
  "bands": "Bands",
  "cefazolin_dose": "Cefazolin",
  "clindamycin_dose": "Clindamycin",
  "bun": "BUN",
  "nbp_sys": "Non-invasive Systolic BP",
  "tobramycin_dose": "Tobramycin",
  "rapamycin_dose": "Rapamycin",
  "ceftazidime_dose": "Ceftazidime",
  "bun_to_cr": "BUN to Creatinine Ratio",
  "epinephrine_dose": "Epinephrine",
  "crystalloid_fluid": "Total Crystalloid Fluid",
  "wbc": "WBC",
  "weight": "Weight",
  "admit_weight": "Admission Weight",
  "hemoglobin": "Hemoglobin",
  "spo2": "SpO2",
  "platelets": "Platelets",
  "arterial_ph": "Arterial pH",
  "inr": "INR",
  "rifampin_dose": "Rifampin",
  "dopamine_dose": "Dopamine",
  "nbp_dias": "Non-invasive Diastolic BP",
  "ampicillin_dose": "Ampicillin",
  "creatinine": "Creatinine",
  "gentamicin_dose": "Gentamicin",
  "vancomycin_dose": "Vancomycin",
  "co2": "CO2",
  "bilirubin": "Bilirubin",
  "ast_liver_enzymes": "AST Liver Enzymes",
  "dobutamine_dose": "Dobutamine",
  "gcs": "GCS",
  "levophed_infusion_dose": "Levophed",
  "oxacillin_dose": "Oxacillin_dose",
  "fluids_intake": "Total Fluid Intake",
  "piperacillin_tazbac_dose": "Piperacillin/tazobactam",
  "lactate": "Lactate",
  "lipase": "Lipase",
  "heart_rate": "Heart Rate",
  "urine_output": "Urine Output"
}