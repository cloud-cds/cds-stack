var INFECTIONS = [
"Endocarditis",
"Meningitis",
"Gastrointenstinal Inffections for Real Tho",
"Infections",
"Bacteremia",
"Cellulitis",
"UTI",
"Pneumonia",
"Endocarditis",
"Meningitis",
"Gastrointenstinal",
"Infections",
"Bacteremia",
"Cellulitis",
"UTI",
"Pneumonia"
]

var ALERT_CODES = {
	"100": "TREWScore has passed the Septic Shock Risk Threshold",
	"101": "TREWScore has been elevated for ",
	"200": "All criteria for <b>Severe Sepsis</b> have been met",
	"201": "All criteria for <b>Septic Shock</b> have been met"
}

var EDIT = {
	"sirs": [
		"temperature is normal",
		"heart rate is normal",
		"respiratory rate is normal",
		"wbc is normal"
	],
	"org": [
		"blood pressure is normal",
		"mean arterial pressure is normal",
		"decrease in SBP is normal",
		"respiratory rate is normal",
		"creatinine is normal",
		"bilirubin is normal",
		"platelet count is normal",
		"INR is normal",
		"lactate is normal"
	],
	"tension": [
		"SBP is normal",
		"mean arterial pressure is normal",
		"decrease in SBP is normal"
	],
	"fusion": [
		"lactate is normal"
	]
}

var CONSTANTS = {
	"sus-edit": "severe_sepsis",
	"sirs": "severe_sepsis",
	"org": "severe_sepsis",
	"tension": "septic_shock",
	"fusion": "septic_shock"
}

severe_sepsis = {
	"display_name": "Severe Sepsis Criteria",
	"suspicion_of_infection": {
		"display_name": "Suspicion of Infection",
	},
	"sirs": {
		"display_name": "SIRS Criteria",
		"criteria": [{
			"key": "sirs_temp",
			"criteria_display_name": "Temperature is > 38.3 or < 36.0",
			"dropdown": "Temperature is normal",
			"overrideModal": [{
				"id": "override_temp",
				"header": "Override Temperature",
				"name": "Body Temperature",
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
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 80,
				"value": 20
			}]
		}, {
			"key": "wbc",
			"criteria_display_name": "WBC > 12,000 or < 4,000 or >10% bands",
			"dropdown": "Wbc and/or Bands is normal",
			"overrideModal": [{
				"id": "override_wbc",
				"header": "Override White Blood Count",
				"name": "White Blood Count",
				"step": 100,
				"range": "true",
				"minAbsolute": 2000,
				"maxAbsolute": 15000,
				"values": [4000, 12000]
			},{
				"id": "override_bands",
				"header": "Override Bands",
				"name": "Bands",
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
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 90,
			}]
		}, {
			"key": "mean_arterial_pressure",
			"criteria_display_name": "Mean arterial pressure < 65",
			"dropdown": "Mean arterial pressure is normal",
			"overrideModal": [{
				"id": "override_mean_arterial_pressure",
				"header": "Override Mean Arterial Pressure",
				"name": "Mean Arterial Pressure",
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 65,
			}]
		}, {
			"key": "decrease_in_sbp",
			"criteria_display_name": "Decrease in SBP by > 40 mmHg from the last recorded SBP considered normal for given patient",
			"dropdown": "Decrease in SBP is normal",
			"overrideModal": [{
				"id": "override_decrease_in_sbp",
				"header": "Override Decrease in Systolic Blood Pressure",
				"name": "Decrease in Systolic Blood Pressure",
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
				"step": null,
				"range": null,
				"minAbsolute": null,
				"maxAbsolute": null,
				"value": null,
			}]
		}, {
			"key": "creatinine",
			"criteria_display_name": "Creatinine > 2.0 or Urine Output < 0.5 mL/kg/hour for 2 hours",
			"dropdown": "Creatinine and/or Urine Output is normal",
			"overrideModal": [{
				"id": "override_creatinine",
				"header": "Override Creatinine",
				"name": "Creatinine",
				"step": 0.1,
				"range": "min",
				"minAbsolute": 0.0,
				"maxAbsolute": 4.0,
				"value": 2.0
			},{
				"id": "override_urine_output",
				"header": "Override Urine Output",
				"name": "Urine Output",
				"step": 0.1,
				"range": "max",
				"minAbsolute": 0.0,
				"maxAbsolute": 3.0,
				"value": 0.5,
			}]
		}, {
			"key": "bilirubin",
			"criteria_display_name": "Bilirubin > 2 mg/dL (34.2 mmol/L)",
			"dropdown": "Bilirubin is normal",
			"overrideModal": [{
				"id": "override_bilirubin",
				"header": "Override Bilirubin",
				"name": "Bilirubin",
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
				"step": 100,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 200000,
				"value": 100000,
			}]
		}, {
			"key": "inr",
			"criteria_display_name": "INR > 1.5 or PTT > 60 sec",
			"dropdown": "INR is normal",
			"overrideModal": [{
				"id": "override_inr",
				"header": "Override INR",
				"name": "INR",
				"step": 1000,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 200000,
				"value": 100000,
			},{
				"id": "override_ptt",
				"header": "Override PTT",
				"name": "PTT",
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 120,
				"value": 60
			}]
		}, {
			"key": "lactate",
			"criteria_display_name": "Lactate > 2mmol/: (18.0 mg/dL)",
			"dropdown": "Lactate is normal",
			"overrideModal": [{
				"id": "override_lactate_measurement",
				"header": "Override Lactate Measurement",
				"name": "Lactate Measurement",
				"step": 0.01,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 4,
				"value": 2
			}]
		}]
	}
}

septic_shock = {
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
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 90,
			}]
		}, {
			"key": "mean_arterial_pressure",
			"criteria_display_name": "Mean arterial pressue < 65",
			"dropdown": "Mean Arterial Pressure is normal",
			"overrideModal": [{
				"id": "override_mean_arterial_pressure",
				"header": "Override Mean Arterial Pressure",
				"name": "Mean Arterial Pressure",
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 130,
				"value": 65,
			}]
		}, {
			"key": "decrease_in_sbp",
			"criteria_display_name": "Decrease in SBP by > 40 mmHg from the last recorded SBP considered normal for given patient",
			"dropdown": "Decrease in sbp is normal",
			"overrideModal": [{
				"id": "override_decrease_in_sbp",
				"header": "Override Decrease in Systolic Blood Pressure",
				"name": "Decrease in Systolic Blood Pressure",
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
			"key": "init_lactate",
			"criteria_display_name": "Initial Lactate level is >= 4 mmol/L",
			"dropdown": "Initial Lactate Measurement is normal",
			"overrideModal": [{
				"id": "override_initial_lactate_measurement",
				"header": "Override Initial Lactate Measurement",
				"name": "Initial Lactate Measurement",
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
		"instruction": "Begins after onset of severs sepsis"
	},
	"sev6": {
		"display_name": "Severe Sepsis 6hr Workflow",
		"instruction": "Begins after initial lactate measurement"
	},
	"sep6": {
		"display_name": "Septic Shock 6hr Workflow",
		"instruction": "Begins after onset of septic shock"
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
