var INFECTIONS = [
	"Unknown Source",
	"Endocarditis",
	"Meningitis",
	"Bacteremia",
	"Cellulitis",
	"UTI",
	"Pneumonia",
	"Multiple Sources of Infection",
	"Reset"
]

var DETERIORATIONS = [
	"AFib w/ RVR",
	"Acute MI",
	"Cardiac arrest",
	"CHF",
	"Cardiac Tamponade",
	"Pulmonary Embolus",
	"COPD exacerbation",
	"Pulmonary Edema",
	"Liver Disease",
	"Hypovolemia",
	"Hemorrhage",
	"Cardiogenic Shock",
	"Distributive Shock",
	"Obstructive Shock"
]

var ALERT_CODES = {
	"100": "TREWScore has passed the Septic Shock Risk Threshold",
	"101": "TREWScore has been elevated for ",
	"200": "All criteria for <b>CMS Severe Sepsis</b> have been met",
	"201": "All criteria for <b>CMS Septic Shock</b> have been met",
	"202": "<b>3hrs</b> have passed since <b>Severe Sepsis</b> onset",
	"203": "<b>6hrs</b> have passed since <b>Severe Sepsis</b> onset",
	"204": "<b>6hrs</b> have passed since <b>Septic Shock</b> onset",
	"205": "<b>6hrs</b> have passed since Suspicion of Infection should have been entered, CMS Severe Sepsis criteria have been reset",
	"206": "<span class='suppressed'><b>6hrs</b> have passed since Suspicion of Infection should have been entered, CMS Severe Sepsis Criteria have been reset</span>",
	"300": "Enter Suspicion of Infection: 2/3 CMS Severe Sepsis Criteria (SIRS and Organ Dysfunction) met",
	"301": "Severe Sepsis 3hr bundle intervention(s) need to be completed", 										// Should be prefixed with count of number of severe sepsis 3hr interventions pending.
	"302": "Severe Sepsis 6hr bundle intervention(s) need to be completed", 										// Should be prefixed with count of number of severe sepsis 6hr interventions pending.
	"303": "Septic Shock 6hr bundle intervention(s) need to be completed", 										  // Should be prefixed with count of number of septic shock interventions pending.
	"304": "Severe Sepsis 3hr bundle intervention(s) need to be completed in the next hour",  // Should be prefixed with the number of severe sepsis interventions to be completed before the 3hr window expires.
	"305": "Severe Sepsis 6hr bundle intervention(s) need to be completed in the next hour",  // Should be prefixed with the number of severe sepsis interventions to be completed before the 6hr window expires.
	"306": "Septic Shock 6hr bundle intervention(s) need to be completed in the next hour",    // Should be prefixed with the number of septic shock interventions to be completed before the 6hr window expires.
	"307": "<span class='suppressed'>Enter Suspicion of Infection: 2/3 CMS Severe Sepsis Criteria (SIRS and Organ Dysfunction) met</span>",
	"400": "All criteria for <b>TREWS Severe Sepsis</b> have been met",
	"401": "All criteria for <b>TREWS Septic Shock</b> have been met",
	"402": "<b>3hrs</b> have passed since <b>Severe Sepsis</b> onset",
	"403": "<b>6hrs</b> have passed since <b>Severe Sepsis</b> onset",
	"404": "<b>6hrs</b> have passed since <b>Septic Shock</b> onset",
	"405": "<b>6hrs</b> have passed since Suspicion of Infection should have been entered, TREWS Severe Sepsis Alert has been reset",
	"406": "<span class='suppressed'><b>6hrs</b> have passed since Suspicion of Infection should have been entered, TREWS Severe Sepsis Alert has been reset</span>",
	"500": "Enter Suspicion of Infection: TREWS Severe Sepsis Alert met",
	"501": "Severe Sepsis 3hr bundle intervention(s) need to be completed", 										// Should be prefixed with count of number of severe sepsis 3hr interventions pending.
	"502": "Severe Sepsis 6hr bundle intervention(s) need to be completed", 										// Should be prefixed with count of number of severe sepsis 6hr interventions pending.
	"503": "Septic Shock 6hr bundle intervention(s) need to be completed", 										  // Should be prefixed with count of number of septic shock interventions pending.
	"504": "Severe Sepsis 3hr bundle intervention(s) need to be completed in the next hour",  // Should be prefixed with the number of severe sepsis interventions to be completed before the 3hr window expires.
	"505": "Severe Sepsis 6hr bundle intervention(s) need to be completed in the next hour",  // Should be prefixed with the number of severe sepsis interventions to be completed before the 6hr window expires.
	"506": "Septic Shock 6hr bundle intervention(s) need to be completed in the next hour",    // Should be prefixed with the number of septic shock interventions to be completed before the 6hr window expires.
	"507": "<span class='suppressed'>Enter Suspicion of Infection: TREWS Severe Sepsis Alert met</span>",
	"600":	"Patient has been manually overridden for Severe Sepsis",
	"601":	"Patient has been manually overridden for Septic Shock",
	"602":	"3hrs have passed since Severe Sepsis Manual Override",
	"603":	"6hrs have passed since Severe Sepsis Manual Override",
	"604":	"6hrs have passed since Septic Shock Manual Override",
	"701":	"Severe Sepsis 3hr bundle intervention(s) need to be completed",
	"702":	"Severe Sepsis 6hr bundle intervention(s) need to be completed",
	"703":	"Septic Shock 6hr bundle intervention(s) need to be completed",
	"704":	"Severe Sepsis 3hr bundle intervention(s) need to be completed in the next hour",
	"705":	"Severe Sepsis 6hr bundle intervention(s) need to be completed in the next hour",
	"706":	"Septic Shock 6hr bundle intervention(s) need to be completed in the next hour",
}

var LOG_STRINGS = {
	"set_deterioration_feedback": " set <b>other conditions driving deterioration</b> to the following values: ",
	"reset": " <b>reset</b> the patient",
	"override": {
		"clear": " cleared values for ",
		"customized": [
			" set ",
			" to "
		],
		"ordered": [
			" <b>placed a</b> ",
			" - status: "
		]
	},
	"deactivate": " <b>deactivated</b> the patient",
	"activate": " <b>activated</b> the patient",
	"toggle_notifications": " toggled notifications",
	"auto_deactivate": " automatically deactivated the patient",
	"reset_bundle_expired_pats": " reset patients with expired treatment bundles after 72 hrs",
	"reset_noinf_expired_pats": " reset patients with no infection after 72 hrs",
	"reset_soi_pats": " reset patients with expired CMS alerts after 6 hrs"
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

var criteriaKeyToName = {
	"sirs_temp": [
		{"name": "Body Temperature",
		"units": "&#176;C"}
	],
	"heart_rate": [
		{"name": "Heart Rate",
		"units": "/min"}
	],
	"respiratory_rate": [
		{"name": "Respiratory Rate",
		"units": "/min"}
	],
	"wbc": [
		{"name": "White Blood Count",
		"units": "K/uL"},
		{"name": "Bands",
		"units": "%"}
	],
	"blood_pressure": [
		{"name": "Systolic Blood Pressure",
		"units": "mmHg"}
	],
	"mean_arterial_pressure": [
		{"name": "Mean Arterial Pressure",
		"units": "mmHg"}
	],
	"decrease_in_sbp": [
		{"name": "Decrease in Systolic Blood Pressure",
		"units": "mmHg"}
	],
	"respiratory_failure": [
		{"name": "Respiratory Failure: Mechanical Support",
		"units": ""}
	],
	"creatinine": [
		{"name": "Creatinine",
		"units": "mg/dL"},
		{"name": "Urine Output",
		"units": "mL/kg/hour"}
	],
	"bilirubin": [
		{"name": "Bilirubin",
		"units": "mg/dL"}
	],
	"platelet": [
		{"name": "Platelet Count",
		"units": ""}
	],
	"inr": [
		{"name": "INR",
		"units": ""},
		{"name": "PTT",
		"units": "sec"}
	],
	"lactate": [
		{"name": "Lactate",
		"units": "mmol/L"}
	],
	"crystalloid_fluid": [
		{"name": "Fluids", "units": "ml/kg"}
	],
	"systolic_bp": [
		{"name": "Systolic Blood Pressure"}
	],
	"hypotension_dsbp": [
		{"name": "Decrease in Systolic Blood Pressure (for Hypotension)",
		"units": "mmHg"}
	],
	"hypotension_map": [
		{"name": "Mean Arterial Pressure (for Hypotension)",
		"units": "mmHg"}
	],
	"suspicion_of_infection": [
		{"name": "Suspicion of Infection"}
	],
	"antibiotics_order": [
		{"name": "Antibiotics Order"}
	],
	"blood_culture_order": [
		{"name": "Blood Culture Order"}
	],
	"crystalloid_fluid_order": [
		{"name": "Fluids Order"}
	],
	"initial_lactate_order": [
		{"name": "Initial Lactate Order"}
	],
	"repeat_lactate_order": [
		{"name": "Repeat Lactate Order"}
	],
	"vasopressors_order": [
		{"name": "Vasopressors Order"}
	],
	/* TREWScore */
	"trews": [
		{"name": "TREWS Acuity Score",
		"units": ""}
	],
	/* TREWS organ dysfunction criteria */
	"trews_sbpm": [
		{"name": "Systolic Blood Pressure",
		"units": "mmHg"}
	],
	"trews_map": [
		{"name": "Mean Arterial Pressure",
		"units": "mmHg"}
	],
	"trews_dsbp": [
		{"name": "Decrease in Systolic Blood Pressure",
		"units": "mmHg"}
	],
	"trews_vent": [
		{"name": "Respiratory Failure: Mechanical Support",
		"units": ""}
	],
	"trews_creatinine": [
		{"name": "Creatinine",
		"units": "mg/dL"},
		{"name": "Urine Output",
		"units": "mL/kg/hour"}
	],
	"trews_bilirubin": [
		{"name": "Bilirubin",
		"units": "mg/dL"}
	],
	"trews_platelet": [
		{"name": "Platelet Count",
		"units": ""}
	],
	"trews_inr": [
		{"name": "INR",
		"units": ""},
		{"name": "PTT",
		"units": "sec"}
	],
	"trews_lactate": [
		{"name": "Lactate",
		"units": "mmol/L"}
	],
	"trews_gcs": [
		{"name": "GCS",
		"units": ""}
	],
	"trews_vasopressors": [
		{"name": "Vasopressors",
		"units": ""}
	],
	/* UI Components */
	"ui_deactivate": [
		{"name": "Notifications to Snooze",
		"units": ""}
	],
	"ui_severe_sepsis": [
		{"name": "Severe Sepsis Bundle",
		"units": ""}
	],
	"ui_septic_shock": [
		{"name": "Septic Shock Bundle",
		"units": ""}
	],
}

var severe_sepsis = {
	"display_name": "Severe Sepsis Evaluation",
	"suspicion_of_infection": {
		// "display_name": "Suspected Source of Infection"
		"display_name": "Please indicate whether infection is suspected"
	},
	"sirs": {
		"key": "sirs",
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
				"values": [36.0, 38.3],
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 3
			"fixed": 1
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
				"range": "max",
				"minAbsolute": 20,
				"maxAbsolute": 240,
				"value": 90,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 4
			"fixed": 1
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
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 80,
				"value": 20,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 3
			"fixed": 1
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
				"values": [4, 12],
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			},{
				"id": "override_bands",
				"header": "Override Bands",
				"name": "Bands",
				"units": "%",
				"step": 0.1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 15,
				"value": 10,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 3
			"fixed": 1
		}]
	},
	"organ_dysfunction": {
		"key": "org",
		"display_name": "CMS Organ Dysfunction Criteria",
		"criteria": [{
			"key": "blood_pressure",
			"criteria_display_name": "Systolic Blood Pressure < 90",
			"dropdown": "Blood pressure is normal",
			"overrideModal": [{
				"id": "override_bp",
				"header": "Override Blood Pressure",
				"name": "Systolic Blood Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 90,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 4
			"fixed": 1
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
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 65,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 3
			"fixed": 1
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
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 100,
				"value": 40,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 3
			"fixed": 1
		}, {
			"key": "respiratory_failure",
			"criteria_display_name": "Acute respiratory failure evidenced by invasive or non-invasive ventiliation",
			"dropdown": "Respiratory failure is normal",
			"overrideModal": [{
				"id": "override_respiratory_failure",
				"header": "Override Respiratory Failure",
				"name": "Respiratory Failure (Mechanical Ventilation Initiated)",
				"units": "",
				"step": 1,
				"range": 'max',
				"minAbsolute": 0,
				"maxAbsolute": 1,
				"value": 0,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			"precision": 1
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
				"range": "max",
				"minAbsolute": 0.0,
				"maxAbsolute": 4.0,
				"value": 2.0,
				"acute_threshold": 0.3,
				"acute_cmp": "GT",
				"acute_arrow": "up"
			}, {
				"id": "override_urine_output",
				"header": "Override Urine Output",
				"name": "Urine Output",
				"units": "mL/kg/hour",
				"step": 0.1,
				"range": "min",
				"minAbsolute": 0.0,
				"maxAbsolute": 3.0,
				"value": 0.5,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 3
			"fixed": 1
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
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 4,
				"value": 2,
				"acute_threshold": 0.3,
				"acute_cmp": "GT",
				"acute_arrow": "up"
			}],
			//"precision": 3
			"fixed": 1
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
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 200000,
				"value": 100000,
				"acute_threshold": -50,
				"acute_cmp": "LT",
				"acute_arrow": "down"
			}],
			//"precision": 6
			"fixed": 0
		}, {
			"key": "inr",
			"criteria_display_name": "INR > 1.5 or PTT > 60 sec",
			"dropdown": "INR is normal",
			"overrideModal": [{
				"id": "override_inr",
				"header": "Override INR",
				"name": "INR",
				"units": "",
				"step": 0.1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 15,
				"value": 1.5,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			},{
				"id": "override_ptt",
				"header": "Override PTT",
				"name": "PTT",
				"units": "sec",
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 120,
				"value": 60,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 4
			"fixed": 1
		}, {
			"key": "lactate",
			"criteria_display_name": "Lactate > 2mmol/L: (18.0 mg/dL)",
			"dropdown": "Lactate is normal",
			"overrideModal": [{
				"id": "override_lactate_measurement",
				"header": "Override Lactate Measurement",
				"name": "Lactate",
				"units": "mmol/L",
				"step": 0.01,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 4,
				"value": 2,
				"acute_threshold": 0.1,
				"acute_cmp": "GT",
				"acute_arrow": "up"
			}],
			//"precision": 3
			"fixed": 1
		}],
		"criteria_mapping": [{
			"src": "blood_pressure",
			"dst": "trews_sbpm"
		}, {
			"src": "mean_arterial_pressure",
			"dst": "trews_map"
		}, {
			"src": "decrease_in_sbp",
			"dst": "trews_dsbp"
		}, {
			"src": "respiratory_failure",
			"dst": "trews_vent"
		}, {
			"src": "creatinine",
			"dst": "trews_creatinine"
		}, {
			"src": "bilirubin",
			"dst": "trews_bilirubin"
		}, {
			"src": "platelet",
			"dst": "trews_platelet"
		}, {
			"src": "inr",
			"dst": "trews_inr"
		}, {
			"src": "lactate",
			"dst": "trews_lactate"
		}]
	},
	"trews": {
		"key": "trews",
		"display_name": "TREWS Acuity Score",
		"criteria": [{
			"key": "trews",
			"criteria_display_name": "TREWS Acuity Score indicates high risk of deterioration",
			"dropdown": "TREWS Acuity Score is normal",
			"overrideModal": [{
				"id": "override_trews",
				"header": "Override TREWS Score assessment",
				"name": "TREWS Acuity Score",
				"units": "",
				"step": 0.05,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 1,
				"value": 0.75,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}]
		}]
	},
	"trews_organ_dysfunction": {
		"key": "trews_org",
		"display_name": "TREWS Organ Dysfunction Criteria",
		"criteria": [{
			"key": "trews_sbpm",
			"criteria_display_name": "Systolic Blood Pressure < 90",
			"dropdown": "Blood pressure is normal",
			"overrideModal": [{
				"id": "override_trews_sbpm",
				"header": "Override Blood Pressure",
				"name": "Systolic Blood Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 90,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 4
			"fixed": 1
		}, {
			"key": "trews_map",
			"criteria_display_name": "Mean arterial pressure < 65",
			"dropdown": "Mean arterial pressure is normal",
			"overrideModal": [{
				"id": "override_trews_map",
				"header": "Override Mean Arterial Pressure",
				"name": "Mean Arterial Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 65,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 3
			"fixed": 1
		}, {
			"key": "trews_dsbp",
			"criteria_display_name": "Decrease in SBP by > 40 mmHg from the last recorded SBP considered normal for given patient",
			"dropdown": "Decrease in SBP is normal",
			"overrideModal": [{
				"id": "override_trews_dsbp",
				"header": "Override Decrease in Systolic Blood Pressure",
				"name": "Decrease in Systolic Blood Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 100,
				"value": 40,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 3
			"fixed": 1
		}, {
			"key": "trews_vent",
			"criteria_display_name": "Acute respiratory failure evidenced by invasive or non-invasive ventiliation",
			"dropdown": "Respiratory failure is normal",
			"overrideModal": [{
				"id": "override_trews_vent",
				"header": "Override Respiratory Failure",
				"name": "Respiratory Failure (Mechanical Ventilation Initiated)",
				"units": "",
				"step": 1,
				"range": 'max',
				"minAbsolute": 0,
				"maxAbsolute": 1,
				"value": 0,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			"precision": 1
		}, {
			"key": "trews_creatinine",
			"baseline_key": "baseline_creatinine",
			"criteria_display_name": "Creatinine > 1.5 mg/dL",
			"baseline_display_name": "0.5 mg/dL increase",
			"baseline_trend": "increased",
			"dropdown": "Creatinine is normal",
			"overrideModal": [{
				"id": "override_trews_creatinine",
				"header": "Override Creatinine",
				"name": "Creatinine",
				"units": "mg/dL",
				"step": 0.1,
				"range": "max",
				"minAbsolute": 0.0,
				"maxAbsolute": 4.0,
				"value": 1.5,
				"acute_threshold": 0.3,
				"acute_cmp": "GT",
				"acute_arrow": "up"
			}],
			//"precision": 3
			"fixed": 1
		}, {
			"key": "trews_bilirubin",
			"baseline_key": "baseline_bilirubin",
			"criteria_display_name": "Bilirubin > 2 mg/dL (34.2 mmol/L)",
			"baseline_display_name": "100%",
			"baseline_trend": "increased",
			"dropdown": "Bilirubin is normal",
			"overrideModal": [{
				"id": "override_trews_bilirubin",
				"header": "Override Bilirubin",
				"name": "Bilirubin",
				"units": "mg/dL",
				"step": 0.01,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 4,
				"value": 2,
				"acute_threshold": 0.3,
				"acute_cmp": "GT",
				"acute_arrow": "up"
			}],
			//"precision": 3
			"fixed": 1
		}, {
			"key": "trews_platelet",
			"baseline_key": "baseline_platelets",
			"criteria_display_name": "Platelet count < 100,000",
			"baseline_display_name": "50% decrease",
			"baseline_trend": "decreased",
			"dropdown": "Platelet Count is normal",
			"overrideModal": [{
				"id": "override_trews_platelet",
				"header": "Override Platelet Count",
				"name": "Platelet Count",
				"units": "",
				"step": 100,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 200000,
				"value": 100000,
				"acute_threshold": -50,
				"acute_cmp": "LT",
				"acute_arrow": "down"
			}],
			//"precision": 6
			"fixed": 0
		}, {
			"key": "trews_inr",
			"baseline_key": "baseline_inr",
			"criteria_display_name": "INR > 1.5 or PTT > 60 sec",
			"baseline_display_name": "0.5 INR increase",
			"baseline_trend": "increased",
			"dropdown": "INR is normal",
			"overrideModal": [{
				"id": "override_trews_inr",
				"header": "Override INR",
				"name": "INR",
				"units": "",
				"step": 0.1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 15,
				"value": 1.5,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			},{
				"id": "override_trews_ptt",
				"header": "Override PTT",
				"name": "PTT",
				"units": "sec",
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 120,
				"value": 60,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			//"precision": 4
			"fixed": 1
		}, {
			"key": "trews_lactate",
			"criteria_display_name": "Lactate > 2mmol/L: (18.0 mg/dL)",
			"dropdown": "Lactate is normal",
			"overrideModal": [{
				"id": "override_trews_lactate",
				"header": "Override Lactate Measurement",
				"name": "Lactate",
				"units": "mmol/L",
				"step": 0.01,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 4,
				"value": 2,
				"acute_threshold": 0.1,
				"acute_cmp": "GT",
				"acute_arrow": "up"
			}],
			//"precision": 3
			"fixed": 1
		}, {
			"key": "trews_gcs",
			"criteria_display_name": "GCS < 13",
			"dropdown": "GCS is normal",
			"overrideModal": [{
				"id": "override_trews_gcs",
				"header": "Override GCS",
				"name": "GCS",
				"units": "",
				"step": 1,
				"range": "min",
				"minAbsolute": 3,
				"maxAbsolute": 15,
				"value": 13,
				"acute_threshold": 2,
				"acute_cmp": "GT",
				"acute_arrow": "up"
			}],
			//"precision": 1
			"fixed": 0
		}, {
			"key": "trews_vasopressors",
			"criteria_display_name": "Vasopressors Initiated",
			"dropdown": "Vasopressors is normal",
			"overrideModal": [{
				"id": "override_trews_vasopressors",
				"header": "Override Vasopressors",
				"name": "Vasopressors Initiated",
				"units": "",
				"step": 1,
				"range": 'max',
				"minAbsolute": 0,
				"maxAbsolute": 1,
				"value": 0,
				"acute_threshold": null,
				"acute_cmp": null,
				"acute_arrow": null
			}],
			"precision": 1
		}],
		"criteria_mapping": [{
			"src": "trews_sbpm",
			"dst": "blood_pressure"
		}, {
			"src": "trews_map",
			"dst": "mean_arterial_pressure"
		}, {
			"src": "trews_dsbp",
			"dst": "decrease_in_sbp"
		}, {
			"src": "trews_vent",
			"dst": "respiratory_failure"
		}, {
			"src": "trews_creatinine",
			"dst": "creatinine"
		}, {
			"src": "trews_bilirubin",
			"dst": "bilirubin"
		}, {
			"src": "trews_platelet",
			"dst": "platelet"
		}, {
			"src": "trews_inr",
			"dst": "inr"
		}, {
			"src": "trews_lactate",
			"dst": "lactate"
		}, {
			"src": "trews_gcs",
			"dst": null
		}, {
			"src": "trews_vasopressors",
			"dst": null
		}]
	}
}

var septic_shock = {
	"display_name": "Septic Shock Monitoring",
	"tension": {
		"key": "tension",
		"display_name": "Persistent Hypotension",
		"criteria": [{
			"key": "hypotension_sbp",
			"criteria_display_name": "Systolic blood pressure (SBP) < 90",
			"dropdown": "Systolic Blood Pressure is normal",
			"overrideModal": [{
				"id": "override_sbp",
				"header": "Override Systolic Blood Pressure",
				"name": "Systolic Blood Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 180,
				"value": 90
			}],
			"precision": 4
		}, {
			"key": "hypotension_map",
			"criteria_display_name": "Mean arterial pressure < 65",
			"dropdown": "Mean Arterial Pressure is normal",
			"overrideModal": [{
				"id": "override_mean_arterial_pressure",
				"header": "Override Mean Arterial Pressure",
				"name": "Mean Arterial Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "min",
				"minAbsolute": 0,
				"maxAbsolute": 130,
				"value": 65
			}],
			"precision": 3
		}, {
			"key": "hypotension_dsbp",
			"criteria_display_name": "Decrease in SBP by > 40 mmHg from the last recorded SBP considered normal for given patient",
			"dropdown": "Decrease in sbp is normal",
			"overrideModal": [{
				"id": "override_decrease_in_sbp",
				"header": "Override Decrease in Systolic Blood Pressure",
				"name": "Decrease in Systolic Blood Pressure",
				"units": "mmHg",
				"step": 1,
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 80,
				"value": 40
			}],
			"precision": 4
		}]
	},
	"fusion": {
		"key": "fusion",
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
				"range": "max",
				"minAbsolute": 0,
				"maxAbsolute": 8,
				"value": 4
			}],
			"precision": 2
		}]
	}
}

workflows = {
	"sev3": {
		"display_name": "Severe Sepsis 3hr Interventions",
		"instruction": "Begins after onset of severe sepsis",
		"not_yet": "No severe sepsis, no action required."
	},
	"sev6": {
		"display_name": "Severe Sepsis 6hr Interventions",
		"instruction": "Begins after initial lactate measurement",
		"not_yet": "No severe sepsis, no action required."
	},
	"sep6": {
		"display_name": "Septic Shock 6hr Interventions",
		"instruction": "Begins after onset of septic shock",
		"not_yet": "No septic shock, no action required."
	},
	"init_lactate": {
		"display_name": "Initial Lactate",
		"as_dose": false
	},
	"blood_culture": {
		"display_name": "Blood Culture",
		"as_dose": false
	},
	"antibiotics": {
		"display_name": "Antibiotics",
		"as_dose": true
	},
	"fluid": {
		"display_name": "Fluid",
		"as_dose": true
	},
	"repeat_lactate": {
		"display_name": "Repeat Lactate",
		"as_dose": false
	},
	"vasopressors": {
		"display_name": "Vasopressors",
		"as_dose": true
	}
}

var doseLimits = {
	'antibiotics': 0,
	'fluid': 0,
	'vasopressors': 0
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

var PHYS_FEATS = ["BP", "temperature", "heart rate", "SpO2", "PaO2", "PaCO2", "resp rate", "FiO2", "GCS", "RASS"];

var HEM_FEATS = ["platelets", "WBC", "INR", "hemoglobin"];

var CHEM_FEATS = ["sodium", "creatinine","bilirubin", "amylase", "lactate", "BUN", "ALT liver enzymes", "arterial ph", "bicarbonate", "CO2", "AST liver enzymes", "potassium", "lipase"];

var DISPLAY_NAMES = {"ALT liver enzymes": "ALT", "AST liver enzymes": "AST", 'temperature':'Temperature', "heart rate": "Heart Rate", "resp rate": "Resp. Rate", "platelets": "Platelets","hematocrit":"hematocrit","hemoglobin":"Hemoglobin","sodium": "Sodium", "creatinine":"Creatinine", "bilirubin":"Bilirubin","amylase":"Amylase", "lactate":"Lactate","arterial ph":"Arterial PH","bicarbonate":"Bicarbonate","potassium":"Potassium","lipase":"Lipase"};
