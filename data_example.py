suspicion_of_infection = {
    "name": "suspicion_of_infection",
    "value": "No Infection",
    "update_time": None,
    "update_user": "user"
}
sirs = {
    "name": "sirs",
    "is_met": False,
    "onset_time": None,
    "num_met": 0,
    "criteria": [
        {
            "name": "sirs_temp",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "heart_rate",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "respiratory_rate",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "wbc",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        }
    ]
}
organ_dysfunction = {
    "name": "organ_dysfunction",
    "is_met": False,
    "onset_time": None,
    "num_met": 0,
    "criteria": [
        {
            "name": "blood_pressure",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "mean_arterial_pressure",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "decrease_in_sbp",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "respiratory_failure",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "creatinine",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "bilirubin",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "platelet",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "inr",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "lactate",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        }
    ]
}
severe_sepsis = {
    "name": "severe_sepsis",
    "is_met": False,
    suspicion_of_infection['name']:suspicion_of_infection,
    sirs["name"]: sirs,
    organ_dysfunction['name']: organ_dysfunction
}

hypotension = {
    "name": "hypotension",
    "is_met": False,
    "onset_time": None,
    "num_met": 0,
    "criteria": [
        {
            "name": "systolic_bp",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "hypotension_map",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        },
        {
            "name": "hypotension_dsbp",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        }
    ]
}
hypoperfusion = {
    "name": "hypoperfusion",
    "is_met": False,
    "onset_time": None,
    "num_met": 0,
    "criteria": [
        {
            "name": "initial_lactate",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
        }
    ]
}
crystalloid_fluid = {
            "name": "crystalloid_fluid",
            "is_met": False,
            "value": None,
            "measurement_time": None,
            "override_time": None,
            "override_user": None
}
septic_shock = {
    "name": "septic_shock",
    "is_met": False,
    "onset_time": None,
    "crystalloid_fluid": crystalloid_fluid,
    hypotension["name"]: hypotension,
    hypoperfusion['name']: hypoperfusion
}

initial_lactate_order = {
    "name": "initial_lactate_order",
    "status": "status string",
    "time": None,
    "user": "user",
    "note": "note"
}
blood_culture_order = {
    "name": "blood_culture_order",
    "status": "status string",
    "time": None,
    "user": "user",
    "note": "note"
}
antibiotics_order = {
    "name": "antibiotics_order",
    "status": "status string",
    "time": None,
    "user": "user",
    "note": "note"
}
crystalloid_fluid_order = {
    "name": "crystalloid_fluid_order",
    "status": "status string",
    "time": None,
    "user": "user",
    "note": "note"
}
repeat_lactate_order = {
    "name": "repeat_lactate_order",
    "status": "status string",
    "time": None,
    "user": "user",
    "note": "note"
}
vasopressors_order = {
    "name": "vasopressors_order",
    "status": "status string",
    "time": None,
    "user": "user",
    "note": "note"
}

patient_data_example = {
    'pat_id': 'patient_example',
    severe_sepsis['name']: severe_sepsis,
    septic_shock['name']: septic_shock,
    initial_lactate_order['name']: initial_lactate_order,
    blood_culture_order['name']: blood_culture_order,
    antibiotics_order['name']: antibiotics_order,
    crystalloid_fluid_order['name']: crystalloid_fluid_order,
    repeat_lactate_order['name']: repeat_lactate_order,
    vasopressors_order['name']: vasopressors_order,
    'chart_data': {
        'chart_values': {
            'timestamp': [1396905900,1396906415,1396908960,1396909920,1396911300,1396911360,1396911420,1396912140,1396913400,1396914960,1396915080,1396915200,1396915740,1396916520,1396917000,1396917900,1396918080,1396918800,1396919700,1396919880,1396919940,1396920000,1396920840,1396922400,1396922940,1396923300,1396924200,1396925100,1396926000,1396926900,1396927800,1396928700,1396929600,1396929900,1396930260,1396930500,1396931400,1396932300,1396933020,1396933200,1396934100,1396934640,1396934940,1396935000,1396935600,1396935900,1396936080,1396936620,1396936800,1396937700,1396937820,1396938600,1396939380,1396939500,1396940400,1396941300,1396942200,1396943100,1396944000,1396944840,1396944900,1396945800,1396946460,1396946700,1396947120,1396947420,1396947600,1396947900,1396948140,1396949220,1396949940,1396950780,1396951200,1396952100,1396953000,1396953900,1396954320,1396954800,1396956840,1396956960,1396957380,1396957740,1396958040,1396958340,1396958400,1396958820,1396959000,1396960200,1396961100,1396961400,1396962000,1396962900,1396963800,1396964700,1396965600,1396966500,1396967220,1396968300,1396969200,1396970100,1396971000,1396971900,1396972200,1396972320,1396972680,1396972800,1396973700,1396974000,1396974300,1396974420,1396974540,1396974660,1396975500,1396975560,1396976400,1396977300,1396978200,1396978740,1396980000,1396980900,1396981800,1396981980,1396982940,1396983600,1396984440,1396984500,1396985400,1396986300,1396987200,1396988100,1396988340,1396989000,1396989900,1396990020,1396990260,1396990380,1396990800,1396991700,1396991820,1396992600,1396993500,1396994160,1396994400,1396995300,1396996200,1396997100,1396998000,1396998900,1396999800,1397000700,1397001600,1397001900,1397002200,1397004300,1397005200,1397005260,1397006100,1397007000,1397007900,1397008800,1397009700,1397010600,1397011500,1397012400,1397013300,1397014200,1397015100,1397016000,1397018220,1397018340,1397018820,1397019060,1397019600,1397019840,1397019900,1397020500,1397021400,1397022300,1397023200,1397024100,1397025000,1397025900,1397026800,1397027700,1397028600,1397028660,1397029380,1397029500,1397029800,1397030400,1397030940,1397031840,1397033100,1397034000,1397034180,1397035260,1397036700,1397037600,1397038500,1397039400,1397039760,1397040300,1397040360,1397041200,1397042100,1397042460,1397043000,1397043660,1397044020,1397044800,1397045700,1397046600,1397047500,1397047920,1397048400,1397050200,1397052180,1397056860,1397056920,1397059200,1397059560,1397062320,1397064600,1397065320,1397065560,1397067480,1397070000,1397070240,1397070900,1397071800,1397072700,1397073000,1397073600,1397075400,1397076300,1397077200,1397078100,1397079000,1397079900,1397080800,1397081700,1397082600,1397083500,1397084400,1397085300,1397086200,1397087100,1397088000,1397088900,1397089800,1397090400,1397090700,1397091600,1397092500,1397093400,1397094300,1397095200,1397096100,1397097000,1397097900,1397098800,1397099700,1397100600,1397101500,1397102400,1397103300,1397104200,1397105100,1397106000,1397106900,1397107800,1397108700,1397109300,1397109600,1397110500,1397112180,1397113200,1397114100,1397114640,1397115000,1397115900,1397116680,1397116800,1397117100,1397118240,1397118600,1397119500,1397119680,1397120400,1397121300,1397121540,1397123100,1397123520,1397124300,1397125800,1397126160,1397127600,1397128500,1397129400,1397130300,1397130600,1397131200,1397132100,1397133000,1397133900,1397134620,1397135100,1397135820,1397142000,1397145600,1397146140,1397148780,1397150340,1397152800,1397153700,1397154600,1397154960,1397155500,1397156400,1397157300,1397158200,1397159100,1397160000,1397160900,1397161800,1397162700,1397163600,1397164500,1397165400,1397166300,1397166420,1397167200,1397168100,1397169000,1397169900,1397170800,1397171700,1397172600,1397173500,1397174400,1397175300,1397176200,1397177100,1397178000,1397178300,1397178900,1397179800,1397180700,1397181600,1397182500,1397183400,1397184300,1397185200,1397187000,1397188800,1397190600,1397192220,1397192400,1397193300,1397196000,1397199600,1397199660,1397200080,1397201460,1397202120,1397203200,1397206800,1397210400,1397214000,1397216820,1397217600,1397221200,1397224800,1397228400,1397231520,1397242800,1397246400,1397247720,1397250000,1397253600,1397257200,1397260800,1397263140,1397263380,1397268000,1397271600,1397274900,1397275200,1397278800,1397282400,1397284080,1397286060,1397286660,1397289300,1397289600,1397290080,1397293200,1397296800,1397300400,1397304000,1397305260,1397307600,1397311200,1397314800,1397318400,1397319360,1397322000,1397325600,1397329200,1397331420,1397332800,1397336400,1397340000,1397343600,1397347200,1397349960,1397350800,1397354400,1397358000,1397359800,1397361600,1397365200,1397366580,1397367180,1397368800,1397368920,1397372400,1397376000,1397379600,1397383200,1397386800,1397388120,1397389920,1397390400,1397394000,1397397600,1397400900,1397401200,1397402580,1397404800,1397406240,1397407260,1397408400,1397412000,1397415600,1397419200,1397421720,1397422800,1397426400,1397430000,1397433600,1397437200,1397439000,1397440800,1397448000,1397456280,1397456580,1397458260,1397458800,1397462400,1397465640,1397466000,1397469600,1397473200,1397475300,1397476800,1397480400,1397484000,1397487600,1397491200,1397492460,1397494800,1397498400,1397502000,1397505600,1397509200,1397512320,1397512800,1397516400,1397520000,1397521560,1397523600,1397527200,1397530800,1397534400,1397538000,1397541600,1397541840,1397544840,1397545200,1397547420,1397548800,1397552400,1397556000,1397559600,1397563200,1397566260,1397566800,1397570400,1397574000,1397577600,1397578680,1397581200,1397584800,1397588400,1397590140,1397590200,1397592000,1397595600,1397596380,1397598600,1397599200,1397602800,1397606400,1397609040,1397610000,1397613600,1397617200,1397620800,1397621580,1397624400,1397628000,1397631600,1397631780,1397632680,1397632920,1397635200,1397638800,1397642400,1397646000,1397649600,1397653200,1397655960,1397656800,1397660400,1397664000,1397666040,1397667600,1397670120,1397671200,1397674800,1397678400,1397682000,1397685060,1397685600,1397689200,1397692800,1397693700,1397696400,1397700000,1397703600,1397706720,1397707200,1397709000,1397710800,1397714400,1397717760,1397717820,1397718000,1397721180,1397721600,1397725200,1397728800,1397732400,1397733420,1397736000,1397739600,1397743200,1397746800,1397750400,1397753220,1397754000,1397757600,1397761200,1397764800,1397765340,1397768400,1397772000,1397775600,1397779200,1397782800,1397784540,1397786400,1397790000,1397793600,1397797200,1397800200,1397800800,1397802300,1397804340,1397804400,1397808000,1397808180,1397811600,1397815200,1397817300,1397818800,1397822400,1397823780,1397826000,1397829600,1397833200,1397835660,1397836800,1397840400,1397844000,1397847600,1397851200,1397854800,1397858400,1397862000,1397865600,1397869020,1397869200,1397872800,1397876400,1397879400,1397880000,1397882160,1397883600,1397886900,1397887200,1397889960,1397890800,1397894400,1397898000,1397898240,1397898300,1397900880,1397905200,1397905920,1397907060,1397908800,1397912400,1397916000,1397919600,1397923200,1397924340,1397925000,1397930400,1397934000,1397934300,1397937600,1397941200,1397944800,1397948400,1397950980,1397952000,1397955600,1397959200,1397962800,1397966400,1397970000,1397972160,1397973600,1397977200,1397980800,1397984400,1397988000,1397991600,1397995200,1398008820,1398029880,1398038340,1398056100,1398056400,1398057300,1398067500,1398069600,1398082260,1398096180,1398109020,1398115680,1398131100,1398146400,1398148560,1398148620,1398153360,1398159120,1398166200,1398181200,1398196260,1398209640,1398228600,1398231840,1398232740,1398239760,1398253920,1398266520,1398280500,1398296700,1398301200,1398312180,1398313320,1398315000,1398324720,1398325080,1398328680,1398328920,1398337200,1398354660,1398357480,1398372600,1398382080,1398396540,1398397620,1398398760,1398413640,1398420720,1398426840,1398442680],
            'trewscore': [0.390534,0.425648,0.447284,0.439287,0.439293,0.439293,0.478555,0.486337,0.501729,0.501735,0.491568,0.437443,0.449644,0.449647,0.440346,0.473489,0.47349,0.475328,0.473122,0.473123,0.473123,0.473123,0.473551,0.468575,0.467667,0.468833,0.467388,0.467469,0.445861,0.443795,0.449869,0.455815,0.429597,0.429599,0.430689,0.447224,0.448812,0.453493,0.458478,0.447979,0.446619,0.436962,0.436526,0.439094,0.439097,0.436016,0.436017,0.532236,0.581042,0.589054,0.585199,0.594984,0.579898,0.578703,0.596909,0.569088,0.549412,0.555996,0.518339,0.510848,0.527733,0.548981,0.549712,0.549713,0.549715,0.550116,0.567656,0.559594,0.562939,0.560299,0.555261,0.556635,0.601205,0.593695,0.589231,0.597888,0.602284,0.602286,0.572068,0.578606,0.588416,0.595649,0.606327,0.600602,0.578955,0.577586,0.577586,0.583253,0.562013,0.554197,0.561885,0.563951,0.565475,0.518685,0.538293,0.537255,0.535541,0.504294,0.501472,0.500557,0.50106,0.473704,0.473706,0.473706,0.473708,0.472788,0.470156,0.470157,0.469192,0.469192,0.476402,0.471025,0.467595,0.446326,0.44633,0.442407,0.442215,0.442563,0.473313,0.456824,0.455484,0.454565,0.45365,0.452733,0.456618,0.455658,0.455425,0.503915,0.43117,0.43252,0.427586,0.456966,0.456061,0.456062,0.452504,0.440137,0.390271,0.390849,0.387853,0.39021,0.424269,0.417819,0.378855,0.378859,0.380679,0.380682,0.35866,0.391476,0.388812,0.375347,0.37535,0.367217,0.370921,0.371817,0.371443,0.372017,0.374657,0.37406,0.376169,0.441447,0.439468,0.440854,0.441431,0.484706,0.485226,0.483933,0.483937,0.497197,0.49947,0.489524,0.489526,0.502989,0.525629,0.496814,0.493535,0.495377,0.483803,0.494636,0.488631,0.487715,0.486422,0.486426,0.487923,0.487926,0.475526,0.482065,0.479199,0.478854,0.478855,0.48384,0.484762,0.486259,0.489443,0.49114,0.491365,0.489753,0.492911,0.510538,0.51238,0.509594,0.500801,0.493992,0.493992,0.493995,0.467634,0.470053,0.469332,0.465776,0.462882,0.454507,0.449685,0.451378,0.444496,0.444498,0.4445,0.463127,0.462789,0.418602,0.410776,0.40978,0.393937,0.392077,0.4175,0.417503,0.413251,0.389082,0.388369,0.385957,0.38504,0.385464,0.38467,0.366618,0.386552,0.430745,0.432588,0.432591,0.431675,0.428739,0.428971,0.428747,0.444595,0.446092,0.444602,0.444032,0.444609,0.444039,0.443697,0.390459,0.390463,0.389893,0.381388,0.381389,0.379208,0.380211,0.381134,0.378645,0.397622,0.399119,0.402801,0.406019,0.403955,0.403731,0.403617,0.403393,0.401967,0.404877,0.401628,0.403391,0.367895,0.365486,0.367556,0.365839,0.366415,0.420578,0.420581,0.421689,0.421693,0.42319,0.429833,0.42926,0.430757,0.411417,0.413708,0.412216,0.408437,0.408438,0.413215,0.399427,0.39943,0.398737,0.399312,0.400732,0.397481,0.395991,0.393706,0.395201,0.436818,0.419984,0.421253,0.423669,0.422177,0.433289,0.43501,0.435853,0.434671,0.439192,0.419487,0.40838,0.404133,0.410064,0.411783,0.40187,0.395315,0.395325,0.395329,0.395332,0.403062,0.403064,0.401882,0.401088,0.399599,0.399603,0.410141,0.430777,0.426997,0.430784,0.430214,0.430218,0.429876,0.432814,0.433388,0.433392,0.434661,0.435238,0.433175,0.433672,0.446571,0.446575,0.446579,0.407896,0.407102,0.407557,0.407561,0.407564,0.405806,0.405581,0.406078,0.407921,0.414297,0.407433,0.406517,0.408014,0.407444,0.410763,0.410994,0.405281,0.411383,0.364921,0.346411,0.38983,0.383753,0.376977,0.377307,0.380423,0.374329,0.395986,0.370512,0.359018,0.396047,0.410658,0.366266,0.368119,0.349919,0.377145,0.358901,0.379198,0.351508,0.353124,0.352698,0.373383,0.373169,0.340623,0.340283,0.340284,0.371232,0.37941,0.36551,0.328234,0.32676,0.35715,0.358076,0.34675,0.33937,0.337631,0.330507,0.31993,0.313725,0.351259,0.354711,0.340347,0.346591,0.315337,0.330869,0.334375,0.294198,0.295065,0.287751,0.301928,0.333878,0.327298,0.282219,0.286845,0.320286,0.316303,0.279214,0.279799,0.289354,0.320804,0.325054,0.326782,0.293273,0.319292,0.310718,0.308902,0.338527,0.333027,0.336619,0.350515,0.304647,0.334029,0.337372,0.336266,0.273227,0.276526,0.259109,0.271384,0.310446,0.319331,0.319336,0.272073,0.272567,0.27536,0.282378,0.329467,0.335985,0.324203,0.304282,0.317995,0.355019,0.356372,0.325006,0.329325,0.330811,0.414147,0.383091,0.341283,0.330125,0.334827,0.32537,0.346141,0.307315,0.312985,0.354714,0.34584,0.345274,0.333051,0.328508,0.309462,0.353344,0.313794,0.316667,0.327739,0.329412,0.344497,0.327685,0.320682,0.324748,0.35085,0.354134,0.335523,0.333196,0.334718,0.353575,0.347848,0.342733,0.33196,0.360571,0.356922,0.357288,0.344964,0.346448,0.368784,0.347744,0.411511,0.37134,0.334174,0.334186,0.333143,0.317366,0.337653,0.32792,0.320778,0.32496,0.355688,0.357083,0.352421,0.352421,0.351266,0.338347,0.304822,0.305848,0.35435,0.355296,0.352365,0.360209,0.309326,0.333428,0.321903,0.359341,0.361241,0.367516,0.320346,0.355147,0.350636,0.346386,0.351339,0.299062,0.283945,0.296337,0.277881,0.28122,0.287824,0.287835,0.323222,0.335789,0.344832,0.34484,0.339908,0.335057,0.322037,0.34454,0.313114,0.305072,0.309139,0.307952,0.296629,0.342008,0.343733,0.326043,0.348679,0.345923,0.346509,0.346396,0.312936,0.316539,0.3119,0.333836,0.328952,0.326319,0.331677,0.301563,0.301104,0.293185,0.331069,0.342183,0.341361,0.324913,0.350098,0.349362,0.298221,0.298232,0.300731,0.332811,0.335886,0.337621,0.325939,0.345888,0.322097,0.321652,0.326068,0.308327,0.301458,0.293894,0.29309,0.334026,0.327661,0.31599,0.319329,0.313457,0.321211,0.336013,0.34534,0.337712,0.344778,0.352399,0.356277,0.369891,0.341853,0.341858,0.34579,0.359232,0.364039,0.364049,0.363069,0.352301,0.371184,0.377748,0.336661,0.33853,0.3497,0.33967,0.314534,0.315122,0.307832,0.324134,0.3271,0.327112,0.311045,0.316156,0.305838,0.298601,0.323554,0.307904,0.306572,0.300647,0.298022,0.309133,0.311762,0.311773,0.321858,0.323805,0.32381,0.32117,0.339135,0.355805,0.336258,0.310956,0.31096,0.304923,0.283269,0.28417,0.284963,0.26809,0.268678,0.318635,0.32266,0.32267,0.312361,0.296161,0.313853,0.322083,0.296776,0.312127,0.307505,0.312463,0.313887,0.309458,0.315719,0.319803,0.318967,0.31145,0.337029,0.337076,0.333458,0.344187,0.330667,0.317625,0.308525,0.30826,0.333939,0.341444,0.371248,0.371275,0.334706,0.316461,0.31333,0.307264,0.280875,0.280899,0.287681,0.362812,0.321179,0.319017,0.329078,0.31888,0.297424,0.324162,0.288416,0.305701,0.288088,0.296892,0.274588,0.290346,0.293454,0.286344,0.305945,0.298771,0.304094,0.342969,0.356128,0.438266,0.438277,0.387456,0.392472,0.360455,0.367321,0.367929,0.383974,0.385454,0.356146,0.373845],
            'tf_1_name': ['BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP','BP'],
            'tf_1_value': [120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120,120],
            'tf_2_name': ['Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate','Heart Rate'],
            'tf_2_value': [70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70,70],
            'tf_3_name': ['WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC','WBC'],
            'tf_3_value': [2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2]
        },
        'trewscore_threshold': 0.56,
        'patient_arrival': {
                'timestamp': None
        },
        'septic_shock_onset':{
            'timestamp': None
        },
        'severe_sepsis_onset':{
            'timestamp': None
        }
    },
    "notifications": []
}
