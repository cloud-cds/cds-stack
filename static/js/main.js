/**
 * Checks if object is empty
*/
function isEmpty(object) {
	for(var i in object) {
		return true;
	}
	return false;
}

function isNumber(obj) {
  return Object.prototype.toString.call(obj) === '[object Number]';
}

function isString(obj) {
  return Object.prototype.toString.call(obj) === '[object String]';
}

/**
 * State Tree, Maintains most up to date app information
 * from server.  Source of Truth
*/
var trews = new function() {
	this.data = {};
	this.isTest = false;
	this.setData = function(data) {
		this.data = data;
	}
	this.setNotifications = function(notifications) {
		if (this.data) {
			this.data['notifications'] = notifications;
		} else {
			this.data = {'notifications': notifications}
		}
	}
	this.getCriteria = function(slot) {
		switch(slot) {
			case 'sirs':
				return this.data['severe_sepsis']['sirs']['criteria'];
			case 'org':
				return this.data['severe_sepsis']['organ_dysfunction']['criteria'];
			case 'tension':
				return this.data['septic_shock']['hypotension']['criteria'];
			case 'fusion':
				return this.data['septic_shock']['hypoperfusion']['criteria'];
		}
	}
	this.getSpecificCriteria = function(slot, key) {
		var arr = this.getCriteria(slot);
		for (var i = 0; i < arr.length; i ++) {
			if (arr[i].name == key) {
				return arr[i];
			}
		}
		return null;
	}
	this.getMetCriteria = function(slot) {
		var list = [];
		var criteria = isString(slot) ? this.getCriteria(slot) : slot
		for (var c in criteria) {
			if (criteria[c]['is_met'] == true) {
				list.push(c);
			}
		}
		return list;
	}
	this.getOverriddenCriteria = function(slot) {
		var list = [];
		var criteria = (slot instanceof String) ? this.getCriteria(slot) : slot
		for (var c in criteria) {
			if (criteria[c]['override_user'] != null) {
				list.push(c);
			}
		}
		return list;
	}
};

var notificationRefresher = new function() {
	this.refreshPeriod = 10000;
	this.refreshTimer = null;
	this.init = function() {
		this.poll(this);
	}
	this.poll = function(obj) {
		endpoints.getPatientData('pollNotifications');
		obj.refreshTimer = window.setTimeout(function() { obj.poll(obj); }, obj.refreshPeriod);
	}
	this.terminate = function() {
		if (this.refreshTimer) {
			window.clearTimeout(this.refreshTimer)
			this.refreshTimer = null
		}
	}
}

/**
 * Slot Component
 * @param JSON String with data for a slot
 * @param HTML element of slot wrapper
 * @param HTML element of expand/minimize link to show/hide all criteria
 * @return {String} html for a specific slot
 */
var slotComponent = function(elem, link, constants) {
	this.criteria = {};
	this.elem = elem;
	this.link = link;
	this.constants = constants;
	this.hasOverridenCriteria = function() {
		var list = []
		for (var c in this.criteria) {
			if (this.criteria[c]['override_user'] != null) {
				list.push(c);
			}
		}
		return list;
	}
	this.r = function(json) {
		this.criteria = json['criteria'];
		this.elem.find('h3').text(this.constants['display_name']);
		if (json['is_met']) {
			this.elem.addClass('complete');
		} else {
			this.elem.removeClass('complete');
		}
		this.elem.find('.criteria-overridden').html('');
		for (var c in this.criteria) {
			var component = new criteriaComponent(this.criteria[c], constants['criteria'][c], constants.key);
			if (component.isOverridden) {
				this.elem.find('.criteria-overridden').append(component.r());
			} else {
				this.elem.find('.criteria').append(component.r());
			}
		}
		this.elem.find('.num-text').text(json['num_met'] + " criteria met. ");
		/*
		if (json['num_met'] == 0) {
			this.elem.find('.edit-btn').addClass('hidden');
		}
		*/
		if (this.hasOverridenCriteria().length == 0) {
			this.elem.find('.num-overridden').addClass('hidden');
			this.elem.find('.criteria-overridden').addClass('hidden');
		} else {
			this.elem.find('.num-overridden').text(this.hasOverridenCriteria().length + " customized criteria");
		}
		this.link.unbind();
		this.link.click({elem: this.elem}, function(e) {
			if ($(this).hasClass('hidden')) {
				e.data.elem.find('.status.hidden').removeClass('hidden').addClass('unhidden');
				$(this).text('minimize').removeClass('hidden');
				if (!Modernizr.csstransitions) {
					// TODO figure out animations on ie8
					// $("[data-trews='sir']").find('.status.hidden').animate({
					// 	opacity: 1
					// }, 300 );
				}
			} else {
				e.data.elem.find('.status.unhidden').removeClass('unhidden').addClass('hidden');
				$(this).text('see all').addClass('hidden');
				this.criteriaHidden = true;
				if (!Modernizr.csstransitions) {
					// TODO figure out animations on ie8
					// $("[data-trews='sir']").find('.status.unhidden').animate({
					// 	opacity: 0
					// }, 300 );
				}
			}
		});
	}
}

window.onload = function() {
	endpoints.getPatientData();
	dropdown.init();
	overrideModal.init();
	notifications.init();
	notificationRefresher.init();
	$('#fake-console').text(window.location);
	$('#fake-console').hide();
	$('#show-console').click(function() {
		$('#fake-console').toggle();
	})
	// Bugsnag.notify("ErrorName", "Test Error");
};

window.onerror = function(error, url, line) {
    controller.sendLog({acc:'error', data:'ERR:'+error+' URL:'+url+' L:'+line});
};

/**
 * Endpoints Object handles sending and receing post requests to server.
 * Handles different connectivity states Inlcuding normal use, testing
 * and improper connection.
*/
var endpoints = new function() {
	this.url = (window.location.hostname.indexOf("localhost") > -1) ?
		"http://localhost:8000/api" :
		window.location.protocol + "//" + window.location.hostname + "/api";
	this.numTries = 1;
	this.getPatientData = function(actionType, actionData) {
		$('body').addClass('waiting');
		postBody = {
			q: (getQueryVariable('PATID') === false) ? null : getQueryVariable('PATID'),
			u: (getQueryVariable('EPICUSERID') === false) ? null : cleanUserId(getQueryVariable('EPICUSERID')),
			depid: (getQueryVariable('ENCDEPID') === false) ? null : getQueryVariable('ENCDEPID'),
			actionType: (actionType) ? actionType : null,
			action: (actionData) ? actionData : null
		}
		// console.log(postBody);
		if (getQueryVariable('test') == 'true' || trews.isTest) {
			if (getQueryVariable('console') == 'true')
				//console.log(postBody);
			this.test();
			return;
		}
		if (postBody['q'] == null) {
			$('#loading p').html("No Patient Identifier entered. Please restart application or contact trews-jhu@opsdx.io<br />" + window.location);
			return;
		}
		$.ajax({
			type: "POST",
			url: this.url,
			data: JSON.stringify(postBody),
			dataType: "json"
		}).done(function(result) {
			$('body').removeClass('waiting');
			$('#loading').addClass('done');
			if ( result.hasOwnProperty('trewsData') ) {
				trews.setData(result.trewsData);
				controller.refresh();
				// $('#fake-console').text(result);
			} else if ( result.hasOwnProperty('notifications') ) {
				trews.setNotifications(result.notifications);
				controller.refreshNotifications();
			}
		}).fail(function(result) {
			$('body').removeClass('waiting');
			if (result.status == 400) {
				$('#loading p').html(result.responseJSON['message'] + ".<br/>  Connection Failed<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews-jhu@opsdx.io");
				return;
			}
			endpoints.numTries += 1;
			if (endpoints.numTries > 3) {
				$('#loading p').html("Connection Failed<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews-jhu@opsdx.io");
				$('#test-data').click(function() {
					endpoints.test();
				});
				$('#see-blank').click(function() {
					$('#loading').addClass('done');
				});
			} else {
				$('#loading p').text("Connection Failed. Retrying...(" + endpoints.numTries + ")");
				endpoints.getPatientData();
			}
		});
	};
	this.test = function() {
		trews.isTest = true;
		$.ajax({
			type: 'GET',
			url: 'js/data_example.json',
			contentType: 'json',
			xhrFields: {
				withCredentials: false
			},
			success: function(result) {
				$('#loading').addClass('done');
				trews.setData(result);
				controller.refresh();
			}
		});
	}
}

var controller = new function() {
	this.clean = function() {
		$('.criteria').html('');
	}
	this.refresh = function() {
		this.clean();
		var globalJson = trews.data;
		severeSepsisComponent.render(globalJson["severe_sepsis"]);
		septicShockComponent.render(globalJson["septic_shock"], globalJson['severe_sepsis']['is_met']);
		workflowsComponent.render(
			globalJson["antibiotics_order"],
			globalJson["blood_culture_order"],
			globalJson["crystalloid_fluid_order"],
			globalJson["initial_lactate_order"],
			globalJson["repeat_lactate_order"],
			globalJson["vasopressors_order"],
			globalJson['chart_data']['severe_sepsis_onset']['timestamp'],
			globalJson['chart_data']['septic_shock_onset']['timestamp']);
		graphComponent.refresh(globalJson["chart_data"]);
		notifications.render(globalJson['notifications']);
	}
	this.refreshNotifications = function() {
		var globalJson = trews.data;
		notifications.render(globalJson['notifications']);
	}
	this.displayJSError = function() {
		notificationRefresher.terminate();
		$('#loading').removeClass('done');
		$('#loading p').html("Javascript Error<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews-jhu@opsdx.io");
		$('#test-data').click(function() {
			endpoints.test();
		});
		$('#see-blank').click(function() {
			$('#loading').addClass('done');
		});
	}
	this.sendLog = function(json) {
		$.ajax({
			type: "POST",
			url: "log",
			data: JSON.stringify(json),
			dataType: "json"
		}).done(function(result) {
			controller.displayJSError();
		}).fail(function(result) {
			controller.displayJSError();
		});
	}
}

var severeSepsisComponent = new function() {
	this.sus = {};
	this.ctn = $("[data-trews='severeSepsis']");
	this.susCtn = $("[data-trews='sus']");

	for (var i in INFECTIONS) {
		var s = $('<option></option>').text(INFECTIONS[i]);
		$('.selection select').append(s);
	}

	this.sirSlot = new slotComponent(
		$("[data-trews='sir']"),
		$('#expand-sir'),
		severe_sepsis['sirs']);
	this.orgSlot = new slotComponent(
		$("[data-trews='org']"),
		$('#expand-org'),
		severe_sepsis['organ_dysfunction']);

	this.suspicion = function(json) {
		this.susCtn.find('h3').text(json['display_name']);
		if (this.sus['value'] == null) {
			this.susCtn.find('.status').hide();
		} else {
			this.susCtn.addClass('complete');
			if (this.sus['value'] == 'No Infection') {
				this.susCtn.removeClass('complete');
			}
			this.susCtn.find('.selection').hide();
			this.susCtn.find('.status h4').text(this.sus.value);

			var susMsg = null;
			if ( this.sus['update_user'] ) { susMsg = 'by ' + this.sus['update_user']; }
			if ( this.sus['update_time'] ) {
				susMsg = (susMsg ? susMsg + ' ' : '') + 'at <span title="' + strToTime(new Date(this.sus['update_time']*1000)) + '">' + timeLapsed(new Date(this.sus['update_time']*1000)) + '</span>';
			}
			if ( susMsg ) { this.susCtn.find('.status h5').html(susMsg); }
		}
	}

	this.render = function(json) {
		this.ctn.find('h2').text(severe_sepsis['display_name']);
		if (json['is_met']) {
			this.ctn.addClass('complete');
		} else {
			this.ctn.removeClass('complete');
		}
		this.sus = json['suspicion_of_infection'];
		this.suspicion(severe_sepsis['suspicion_of_infection']);
		this.sirSlot.r(json['sirs']);
		this.orgSlot.r(json['organ_dysfunction']);
	}
}

var septicShockComponent = new function() {
	this.ctn = $("[data-trews='septicShock']");

	this.fnote = $('#fluid-note');
	this.fnoteBtn = $('#fluid-note-btn');

	this.tenSlot = new slotComponent(
		$("[data-trews='tension']"),
		$('#expand-ten'),
		septic_shock['tension']);

	this.fusSlot = new slotComponent(
		$("[data-trews='fusion']"),
		$('#expand-fus'),
		septic_shock['fusion']);

	this.render = function(json, severeSepsis) {
		this.ctn.find('h2').text(septic_shock['display_name']);

		if (json['is_met']) {
			this.ctn.addClass('complete');
		} else {
			this.ctn.removeClass('complete');
		}

		this.tenSlot.r(json['hypotension']);
		this.fusSlot.r(json['hypoperfusion']);

		this.fnoteBtn.unbind();
		if ( json['crystalloid_fluid']['is_met'] ) {
			this.fnote.addClass("complete");
		} else {
			this.fnote.removeClass("complete");
		}

		var fnoteBtnText = null;
		if ( json['crystalloid_fluid']['is_met'] ) {
			if ( json['crystalloid_fluid']['override_user'] ) { fnoteBtnText = "Reset"; }
		} else {
			fnoteBtnText = "Not Indicated";
		}

		if ( fnoteBtnText ) {
			this.fnoteBtn.text(fnoteBtnText);
			var action = {
				"actionName": 'crystalloid_fluid'
			};

			if ( fnoteBtnText == 'Not Indicated' ) {
				action['value'] = 'Not Indicated';
			} else {
				action['clear'] = true;
			}
			this.fnoteBtn.click(function() {
				endpoints.getPatientData("override", [action]);
			});
		} else {
			this.fnoteBtn.hide();
		}

		if (!severeSepsis) {
			this.ctn.addClass('inactive');
		} else {
			this.ctn.removeClass('inactive');
		}
	}
}

var workflowsComponent = new function() {
	this.sev3Ctn = $("[data-trews='sev3']");
	this.sev6Ctn = $("[data-trews='sev6']");
	this.sep6Ctn = $("[data-trews='sep6']");
	this.orderBtns = $('.place-order');
	this.notInBtns = $('.notIn');

	this.clean = function() {
		$("[data-trews='init_lactate'],\
			[data-trews='blood_culture'],\
			[data-trews='antibiotics'],\
			[data-trews='fluid'],\
			[data-trews='re_lactate'],\
			[data-trews='vasopressors']").html(''); //TODO: add focus exam
	}

	this.makeButtons = function() {
		this.orderBtns.unbind();
		this.orderBtns.click(function() {
			endpoints.getPatientData('place_order', {'actionName': $(this).attr('data-trews')});
		});
		this.notInBtns.unbind();
		this.notInBtns.click(function() {
			endpoints.getPatientData('order_not_indicated', {'actionName': $(this).attr('data-trews')});
		});
	}

	this.workflowStatus = function(tag, time) {
		if (time == null) {
			return workflows[tag]['not_yet'];
		}
		var status = workflows[tag]['instruction'];
		if (tag == "sev3") {
			var offset = 3 * 60 * 60 * 1000;
		} else if (tag = "sev6") {
			var offset = 6 * 60 * 60 * 1000;
		} else {
			var offset = 6 * 60 * 60 * 1000;
		}
		var shiftedTime = (time * 1000) + offset;
		var wfDate = new Date(shiftedTime);
		if (shiftedTime < Date.now()) {
			status = "Workflow window over <span title='" + strToTime(wfDate) + "'>" + timeLapsed(wfDate) + "</span>";
		} else {
			status = "<span title='" + strToTime(wfDate) + "'>" + timeRemaining(wfDate) + "</span>";
		}
		return status;
	}


	this.render = function(aJSON, bJSON, fJSON, iJSON, rJSON, vJSON, severeOnset, shockOnset) {
		// this.clean();
		this.sev3Ctn.find('h2').text(workflows['sev3']['display_name']);
		this.sev6Ctn.find('h2').text(workflows['sev6']['display_name']);
		this.sep6Ctn.find('h2').text(workflows['sep6']['display_name']);

		if (severeOnset == null) {
			this.sev3Ctn.addClass('inactive');
			this.sev6Ctn.addClass('inactive');
		} else {
			this.sev3Ctn.removeClass('inactive');
			this.sev6Ctn.removeClass('inactive');
		}
		if (shockOnset == null) {
			this.sep6Ctn.addClass('inactive');
		} else {
			this.sep6Ctn.removeClass('inactive');
		}

		this.sev3Ctn.find('.card-subtitle').html(this.workflowStatus('sev3', severeOnset));
		this.sev6Ctn.find('.card-subtitle').html(this.workflowStatus('sev6', severeOnset));
		this.sep6Ctn.find('.card-subtitle').html(this.workflowStatus('sep6', shockOnset));

		var iTask = new taskComponent(iJSON, $("[data-trews='init_lactate']"), workflows['init_lactate']);
		var bTask = new taskComponent(bJSON, $("[data-trews='blood_culture']"), workflows['blood_culture']);
		var aTask = new taskComponent(aJSON, $("[data-trews='antibiotics']"), workflows['antibiotics']);
		var fTask = new taskComponent(fJSON, $("[data-trews='fluid']"), workflows['fluid']);

		var rTask = new taskComponent(rJSON, $("[data-trews='re_lactate']"), workflows['repeat_lactate']);

		var vTask = new taskComponent(vJSON, $("[data-trews='vasopressors']"), workflows['vasopressors']);

		this.makeButtons();
	}
}

var graphComponent = new function() {
	this.is30 = true;
	this.xmin = 0;
	this.xmax = 0;
	this.ymin = 0;
	this.ymax = 0;
	$("<div id='tooltip'></div>").appendTo("body");
	this.refresh = function(json) {
		this.is30 = true;
		this.render(json);
	}
	this.render = function(json) {
		if (json == undefined) {
			return;
		}
		if (this.is30) {
			var dataLength = json['chart_values']['timestamp'].length;
			for (var i = 0; i < dataLength; i += 1) {
				json['chart_values']['timestamp'][i] *= 1000;
			}
			this.is30 = false;
		}
		this.xmin = json['chart_values']['timestamp'][0];
		this.ymin = 0;//json['chart_values']['trewscore'][0];
		var max = json['chart_values']['timestamp'][json['chart_values']['timestamp'].length - 1];
		this.xmax = ((max - this.xmin) / 6) + max;
		max = json['chart_values']['trewscore'][json['chart_values']['trewscore'].length - 1];
		this.ymax = 1; //((max - this.ymin) / 6) + max;
		// this.ymin = Math.min.apply(null, json['chart_values']['trewscore']);
		// this.ymax = Math.max.apply(null, json['chart_values']['trewscore']) * 1.16;
		graph(json, this.xmin, this.xmax, this.ymin, this.ymax);
	}
	window.onresize = function() {
		graphComponent.render(trews.data.chart_data, graphComponent.xmin, graphComponent.xmax);
	}
}



// Utilities
/**
 * outputs a text friendly output of time elapsed
 * @param Date Object, input time
 * @return {String} formatted time lapsed
 */
function timeLapsed(d) {
	var MIN = 60 * 1000;
	var HOUR = 60 * 60 * 1000;
	var DAY = 24 * 60 * 60 * 1000;
	var elapsed = new Date(Date.now() - d);
	if (elapsed < MIN) {
		var units = (elapsed.getUTCSeconds() > 1) ? " secs ago" : " sec ago";
		return elapsed.getUTCSeconds() + units;
	} else if (elapsed < HOUR) {
		var units = (elapsed.getUTCMinutes() > 1) ? " mins ago" : " min ago";
		return elapsed.getUTCMinutes() + units;
	} else if (elapsed < DAY) {
		var units = (elapsed.getUTCHours() > 1) ? " hrs ago" : " hr ago";
		return elapsed.getUTCHours() + units;
	} else {
		return "on " + d.toLocaleDateString() + " at " + d.toLocaleTimeString();
	}
}
/**
 * outputs a text friendly output of time remaining
 * @param Date Object, input time
 * @return {String} formatted time lapsed
 */
function timeRemaining(d) {
	var remaining = new Date(d - Date.now());
	var minutes = (remaining.getUTCMinutes() < 10) ? "0" + remaining.getUTCMinutes() : remaining.getUTCMinutes();
	return remaining.getUTCHours() + " hours and " + minutes + " minutes remaining";
}

/**
 * String to Time
 * Takes a string converts it to a Date object and outputs date/time
 * as such: m/d/y h:m
*/
function strToTime(str, timeFirst) {
	var date = new Date(Number(str));
	var y = date.getFullYear();
	var m = date.getMonth() + 1;
	var d = date.getDate();
	var h = date.getHours() < 10 ? "0" + date.getHours() : date.getHours();
	var min = date.getMinutes() < 10 ? "0" + date.getMinutes() : date.getMinutes();
	if (timeFirst) {
		return h + ":" + min + " " + m + "/" + d + "/" + y;
	} else {
		return m + "/" + d + "/" + y + " " + h + ":" + min;
	}
}
/**
 * FID to human readable
 * Takes an fid as a string and converts it to its human readable counterpart
*/
function humanReadable(str) {
	if (FID_TO_HUMAN_READABLE[str]) {
    return FID_TO_HUMAN_READABLE[str];
  }
  return str;
}
/**
 * Gets URL Parameters.  Returns the parameter value for the inpuuted variable
 * returns false if no variable found
 * source: https://css-tricks.com/snippets/javascript/get-url-variables/
*/
function getQueryVariable(variable) {
	var query = window.location.search.substring(1);
	var vars = query.split("&");
	for (var i=0;i<vars.length;i++) {
		var pair = vars[i].split("=");
		if (pair[0] == variable){
			return pair[1];
		}
	}
	return(false);
}
/**
 * EPICUSERID is padded with plus signs, this removes them
*/
function cleanUserId(userId) {
	return userId.replace(/^\++/, "");
}

/**
 * Criteria Component
 * @param JSON String
 * @return {String} html for a specific criteria
 */
var criteriaComponent = function(c, constants, key) {
	this.isOverridden = false;
	this.status = "";

	var displayValue = c['value'];
	if ( displayValue && isNumber(displayValue) ) {
		displayValue = displayValue.toPrecision(5);
	}

	// Local conversions.
	if ( c['name'] == 'sirs_temp' ) {
		displayValue = ((Number(displayValue) - 32) / 1.8).toPrecision(3);
	}

	if (c['is_met'] && c['measurement_time']) {
		this.classComplete = " met";
		var lapsed = timeLapsed(new Date(c['measurement_time']*1000));
		var strTime = strToTime(new Date(c['measurement_time']*1000));
		this.status += "Criteria met <span title='" + strTime + "'>" + lapsed + "</span> with a value of <span class='value'>" + displayValue + "</span>";
	} else {
		if (c['override_user'] != null) {
			this.classComplete = " unmet";
			this.isOverridden = true;
			if (c['measurement_time']) {
				var cLapsed = timeLapsed(new Date(c['measurement_time']*1000));
				var cStrTime = strToTime(new Date(c['measurement_time']*1000));
				this.status += "Criteria met <span title='" + cStrTime + "'>" + cLapsed + "</span> with a value of <span class='value'>" + displayValue + "</span>";
				this.status += (c['override_time']) ? "<br />" : "";
			}
			if (c['override_time']) {
				var oLapsed = timeLapsed(new Date(c['override_time']*1000));
				var oStrTime = strToTime(new Date(c['override_time']*1000));
				this.status += "Customized by " + c['override_user'] + " <span title='" + oStrTime + "'>" + oLapsed + "</span>";
			}
		} else {
			this.classComplete = " hidden unmet";
			this.status = "";
		}
	}
	var criteriaString = "";
	for (var i = 0; i < constants.overrideModal.length; i++) {
		var crit = null;
		if (c['override_user'] != null) {
			if (trews.getSpecificCriteria(key, constants.key).override_value[i] != undefined) {
				if (Object.keys(trews.getSpecificCriteria(key, constants.key).override_value[i]).length == 1) {
					crit = trews.getSpecificCriteria(key, constants.key).override_value[i].lower ? trews.getSpecificCriteria(key, constants.key).override_value[i].lower : trews.getSpecificCriteria(key, constants.key).override_value[i].upper;
				} else {
					crit = [trews.getSpecificCriteria(key, constants.key).override_value[i].lower, trews.getSpecificCriteria(key, constants.key).override_value[i].upper]
				}
			}
		} else {
			crit = (constants.overrideModal[i].value) ? constants.overrideModal[i].value : constants.overrideModal[i].values
		}
		var name = constants.overrideModal[i].name
		var unit = constants.overrideModal[i].units
		if (constants.overrideModal[i].range == 'true') {
			criteriaString += name + " < " + crit[0] + unit + " or > " + crit[1] + unit;
		} else {
			var comp = (constants.overrideModal[i].range == 'min') ? ">" : "<";
			criteriaString += name + " " + comp + " " + crit + unit;
		}
		criteriaString += " or ";
	}
	criteriaString = criteriaString.slice(0, -4);
	this.html = "<div class='status" + this.classComplete + "'>\
					<h4>" + criteriaString + "</h4>\
					<h5>" + this.status + "</h5>\
				</div>";
	this.r = function() {
		return this.html;
	}
}

/**
 * Task Component, numerous for each workflow
 * @param JSON String with data for a task
 * @param HTML element of task wrapper
 * @return {String} html for a specific task
 */
var taskComponent = function(json, elem, constants) {
	elem.find('h3').text(constants['display_name']);
	elem.removeClass('in-progress');
	elem.removeClass('complete');
	if ( json['status'] == 'Ordered' ) {
		elem.addClass('in-progress')
	}
	else if ( json['status'] == 'Completed' ) {
		elem.addClass('complete')
	}
}

var dropdown = new function() {
	this.d = $('#dropdown');
	this.ctn = $("<div id='dropdown-content'></div>");
	this.init = function() {
		this.d.append(this.ctn);
	}
	this.reset = function() {
		$('.edit-btn').removeClass('shown');
		this.ctn.html("");
	}
	this.getCtnElem = function(id) {
		return $("[data-trews='" + id + "']");
	}
	this.getAction = function(value) {
		return {
			"actionName": this.d.attr('data-trews'),
			"value": value
		};
	}
	this.getLaunchAction = function(criteria) {
		var slot = this.d.attr('data-trews');
		if ( slot == 'org' ) { slot = 'organ_dysfunction'; }
		return {
			"card": CONSTANTS[this.d.attr('data-trews')],
			"slot": slot,
			"criteria": criteria
		};
	}
	this.sus = function() {
		for (var i in INFECTIONS) {
			var s = $('<h5 class="dropdown-link"></h5>').text(INFECTIONS[i]);
			this.ctn.append(s);
		}
		$('.dropdown-link').click(function() {
			var action = dropdown.getAction($(this).text());
			endpoints.getPatientData("suspicion_of_infection", action);
		});
	}
	this.editFields = function(field) {
		var allCriteria = trews.getCriteria(field);
		var editCriteriaIndices = [];
		for (var i in allCriteria) {
			editCriteriaIndices.push(i);
			var s = $('<h5 class="dropdown-link" data-trews="' + i + '"></h5>');
			var txt = allCriteria[i]['override_user'] ? (EDIT[field][i] + ' is customized') : ('customize ' + EDIT[field][i]);
			var dropdownClass = allCriteria[i]['override_user'] ? 'overridden' : '';
			s.text(txt);
			s.addClass(dropdownClass);
			this.ctn.append(s);
		};
		$('.dropdown-link').click(function(e) {
			var launchAction = dropdown.getLaunchAction($(this).attr('data-trews'));
			overrideModal.launch(launchAction);
		});
	}
	this.fill = function(i) {
		this.d.attr('data-trews', i);
		if (i === 'sus-edit')
			this.sus();
		else
			this.editFields(i);
	}
	this.draw = function(x, y) {
		this.d.css({
			top: y + 7,
			left: x - (this.d.width()/2)
		}).fadeIn(30);
	}
	$('.edit-btn').click(function(e) {
		e.stopPropagation();
		dropdown.reset();
		$(this).addClass('shown');
		dropdown.fill($(this).attr('data-trews'));
		dropdown.draw($(this).offset().left + ($(this).width()/2),
			$(this).offset().top + $(this).height());
	});
	$('body').click(function() {
		$('.edit-btn').removeClass('shown');
		dropdown.d.fadeOut(300);
	});
}

var overrideModal = new function() {
	this.om = $('#override-modal');
	this.ctn = $('<div id="om-content"></div>');
	this.card = "";
	this.slot = "";
	this.criteria = "";
	this.reset = false;
	this.init = function() {
		this.om.append(this.ctn);
	}
	this.modalView = function(data, index) {
		var html = "<h3>" + data['header'] + "</h3>";
		html += "<p>Define a new acceptable range.  The criteria will be met once the patient's " + data['name'] + " falls out of the new range.</p>";
		html += "<div><a class='override-reset' data-trews='" + index + "'>reset</a><span class='slider-numbers' data-trews='" + data['id'] + "'></span></div>"
		html += "<div class='slider-range' data-trews='" + data['id'] + "'></div>";
		// html += "<p>or define a lockout period.  During this lockout period the criteria will not be met.  The criteria will be reevaluated after once the lockout period ends.</p>";
		// html += "<input class='override-lockout' data-trews='" + data['id'] + "' type='num'>";
		return html;
	}
	this.makeSliders = function(data, index) {
		var o = trews.data[this.card][this.slot]['criteria'][this.criteria]['override_value']
		if (data['range'] === "true") {
			$(".slider-range[data-trews='" + data['id'] + "']").slider({
				range: data['range'],
				min: data['minAbsolute'],
				max: data['maxAbsolute'],
				step: data['step'],
				values: [
					(o == null) ? data['values'][0] : (o[index]['lower']) ? o[index]['lower'] : data['values'][0],
					(o == null) ? data['values'][1] : (o[index]['upper']) ? o[index]['upper'] : data['values'][1]
				],
				slide: function( event, ui ) {
					$(".slider-numbers[data-trews='" + data['id'] + "']").text(ui.values[0] + " - " + ui.values[1]);
				}
			});
			$(".slider-numbers[data-trews='" + data['id'] + "']").text($(".slider-range[data-trews='" + data['id'] + "']").slider("values",0) + " - " + $(".slider-range[data-trews='" + data['id'] + "']").slider("values",1));
		} else {
			var oValue = data['value']
			if (data['range'] === "min") {
				slideFunction = function(event, ui) {
					$(".slider-numbers[data-trews='" + data['id'] + "']").text(data['minAbsolute'] + " - " + ui.value);
				}
				oValue = (o == null) ? oValue : (o[index]['lower']) ? o[index]['lower'] : oValue
			} else {
				slideFunction = function(event, ui) {
					$(".slider-numbers[data-trews='" + data['id'] + "']").text(ui.value + " - " + data['maxAbsolute']);
				}
				oValue = (o == null) ? oValue : (o[index]['upper']) ? o[index]['upper'] : oValue
			}
			$(".slider-range[data-trews='" + data['id'] + "']").slider({
				range: data['range'],
				min: data['minAbsolute'],
				max: data['maxAbsolute'],
				step: data['step'],
				value: oValue,
				slide: slideFunction
			});
			if (data['range'] === "min") {
				$(".slider-numbers[data-trews='" + data['id'] + "']").text(data['minAbsolute'] + " - " + $(".slider-range[data-trews='" + data['id'] + "']").slider("value"));
			} else {
				$(".slider-numbers[data-trews='" + data['id'] + "']").text($(".slider-range[data-trews='" + data['id'] + "']").slider("value") + " - " + data['maxAbsolute']);
			}
		}
	}
	this.makeActions = function() {
		var save = $('.override-save');
		var cancel = $('.override-cancel');
		var reset = $('.override-reset');
		save.unbind();
		save.click(function() {
			var sliders = $('.slider-range');
			var postData = {
				"actionName": STATIC[overrideModal.card][overrideModal.slot]['criteria'][overrideModal.criteria]['key'],
				"value": [],
				"clear": false
			};
			for (var i = 0; i < sliders.length; i++) {
				var criteria = sliders[i].getAttribute('data-trews');
				var criteriaOverrideData = STATIC[overrideModal.card][overrideModal.slot]['criteria'][overrideModal.criteria]['overrideModal'][i];
				var criteriaOverride = { "range": criteriaOverrideData['range']}
				if ($(".slider-range[data-trews='" + criteria + "']").slider("values").length == 0) {
					criteriaOverride["lower"] = (criteriaOverrideData['range'] == 'max') ? $(".slider-range[data-trews='" + criteria + "']").slider("value") : null;
					criteriaOverride["upper"] = (criteriaOverrideData['range'] == 'min') ? $(".slider-range[data-trews='" + criteria + "']").slider("value") : null;
				} else {
					criteriaOverride["lower"] = $(".slider-range[data-trews='" + criteria + "']").slider("values", 0);
					criteriaOverride["upper"] = $(".slider-range[data-trews='" + criteria + "']").slider("values", 1);
				}
				postData["clear"] = (overrideModal.reset) ? true : false;
				postData.value.push(criteriaOverride);
			}
			endpoints.getPatientData("override", postData);
			overrideModal.om.fadeOut(30);
		});
		cancel.unbind();
		cancel.click(function() {
			overrideModal.om.fadeOut(30);
		});
		reset.unbind();
		reset.click(function() {
			overrideModal.reset = true;
			var criteriaOverrideData = STATIC[overrideModal.card][overrideModal.slot]['criteria'][overrideModal.criteria]['overrideModal'][$(this).attr('data-trews')];
			if (criteriaOverrideData['range'] == 'true') {
				$(".slider-range[data-trews='" + criteriaOverrideData['id'] + "']").slider("values", criteriaOverrideData['values']);
				$(".slider-numbers[data-trews='" + criteriaOverrideData['id'] + "']").text(criteriaOverrideData['values'][0] + " - " + criteriaOverrideData['values'][1]);
			} else {
				$(".slider-range[data-trews='" + criteriaOverrideData['id'] + "']").slider("value", criteriaOverrideData['value'])
				if (criteriaOverrideData['range'] == 'min') {
					$(".slider-numbers[data-trews='" + criteriaOverrideData['id'] + "']").text(criteriaOverrideData['minAbsolute'] + " - " + criteriaOverrideData['value']);
				} else {
					$(".slider-numbers[data-trews='" + criteriaOverrideData['id'] + "']").text(criteriaOverrideData['value'] + " - " + criteriaOverrideData['maxAbsolute']);
				}
			}
		})
	}
	this.launch = function(action) {
		this.card = action['card'];
		this.slot = action['slot'];
		this.criteria = action['criteria'];
		this.reset = false;
		this.ctn.html("");
		if (!Modernizr.opacity) {
			this.om.addClass('no-opacity');
		}
		var overrideModal = STATIC[action['card']][action['slot']]['criteria'][action['criteria']]['overrideModal'];
		for (var i = 0; i < overrideModal.length; i++) {
			this.ctn.append(this.modalView(overrideModal[i], i));
			this.makeSliders(overrideModal[i], i);
		}
		this.ctn.append("<div id='om-actions'><a class='override-cancel'>Cancel</a><a class='override-save'>Save</a>");
		this.makeActions();
		this.om.fadeIn(30);
	}
	this.om.unbind();
	this.om.click(function() {
		overrideModal.om.fadeOut(30);
	});
	this.ctn.unbind();
	this.ctn.click(function(e) {
		e.stopPropagation();
	});
}

var notifications = new function() {
	this.n = $('#notifications');
	this.nav = $('#header-notifications');
	this.init = function() {
		this.nav.unbind();
		this.nav.click(function(e) {
			e.stopPropagation();
			notifications.n.toggle();
		});
		$('body').click(function() {
			notifications.n.fadeOut(30);
		});
		this.n.unbind();
		this.n.click(function(e) {
			e.stopPropagation();
		});
	}
	this.render = function(data) {
		this.n.html('');
		if (data == undefined) {
			this.n.append('<p class="none">Can\'t retrieve notifications at this time.  <br />Notifications may be under construction.</p>')
			this.nav.find('b').hide();
			this.nav.find('.text').text('Notifications');
			return;
		}
		if (data.length == 0) {
			this.n.append('<p class="none">No Notifications</p>')
			this.nav.find('b').hide();
			this.nav.find('.text').text('Notifications');
			return;
		}
		var numUnread = 0;
		for (var i = 0; i < data.length; i++) {
			var notif = $('<div class="notification"></div>');
			notif.append('<h3>' + ALERT_CODES[data[i]['alert_code']] + '</h3>')
			var subtext = $('<div class="subtext cf"></div>');
			subtext.append('<p>' + timeLapsed(new Date(data[i]['timestamp']*1000)) + '</p>');
			var readLink = $("<a data-trews='" + data[i]['id'] + "'></a>");
			readLink.unbind();
			if (data[i]['read']) {
				notif.addClass('read');
				readLink.text('Mark as unread');
				readLink.click(function() {
					var data = {
						"id":$(this).attr('data-trews'),
						"read":false
					}
					endpoints.getPatientData("notification", data);
				})
			} else {
				numUnread++;
				readLink.text('Mark as read');
				readLink.click(function() {
					var data = {
						"id":$(this).attr('data-trews'),
						"read":true
					}
					endpoints.getPatientData("notification", data);
				})
			}
			subtext.append(readLink);
			notif.append(subtext);
			this.n.prepend(notif);
		}
		if (numUnread == 0) {
			this.nav.find('b').hide();
		} else {
			this.nav.find('b').show();
			this.nav.find('.num').text(numUnread);
		}
		//this.nav.find('.num').text(data.length);  // yanif: commented out with read/unread re-enabled
		if (data.length > 1) {
			this.nav.find('.text').text('Notifications');
		} else {
			this.nav.find('.text').text('Notification');
		}
	}
}

function graph(json, xmin, xmax, ymin, ymax) {
	var graphWidth = Math.floor($('#graph-wrapper').width()) - 10;
	$("#graphdiv").width(graphWidth);
	$("#graphdiv").height(graphWidth * .3225);
	var placeholder = $("#graphdiv");

	var data = [];
	var dataLength = json['chart_values']['timestamp'].length;
	for (var i = 0; i < dataLength; i += 1) {
		data.push([json['chart_values']['timestamp'][i],
			json['chart_values']['trewscore'][i]]);
	}

	// console.log(data, xmin, xmax);

	var xlast = json['chart_values']['timestamp'][dataLength - 1];
	var ylast = json['chart_values']['trewscore'][dataLength - 1];

	// update trewscore in header
	$('h1 span').text(Number(ylast).toFixed(2));

	var verticalMarkings = [
		{color: "#555",lineWidth: 1,xaxis: {from: xlast,to: xlast}}
	]

	var arrivalx = (json['patient_arrival']['timestamp'] != undefined) ? json['patient_arrival']['timestamp'] * 1000 : null;
	var severeOnsetx = (json['severe_sepsis_onset']['timestamp'] != undefined) ? json['severe_sepsis_onset']['timestamp'] * 1000 : null;
	var shockOnsetx = (json['septic_shock_onset']['timestamp'] != undefined) ? json['septic_shock_onset']['timestamp'] * 1000 : null;

	if (json['patient_arrival']['timestamp'] != undefined) {
		var arrivalMark = {color: "#ccc",lineWidth: 1,xaxis: {from: arrivalx,to: arrivalx}};
		//var arrivaly = json['chart_values']['trewscore'].indexOf(arrivalx);
		var arrivaly = jQuery.inArray(arrivalx, json['chart_values']['trewscore'])
		verticalMarkings.push(arrivalMark);
	}
	if (json['severe_sepsis_onset']['timestamp'] != undefined) {
		var severeMark = {color: "#ccc",lineWidth: 1,xaxis: {from: severeOnsetx,to: severeOnsetx}};
		//var severeOnsety = json['chart_values']['trewscore'].indexOf(severeOnsetx);
		var severeOnsety = jQuery.inArray(severeOnsetx, json['chart_values']['trewscore'])
		verticalMarkings.push(severeMark);
	}
	if (json['septic_shock_onset']['timestamp'] != undefined) {
		var shockMark = {color: "#ccc",lineWidth: 1,xaxis: {from: shockOnsetx,to: shockOnsetx}};
		//var shockOnsety = json['chart_values']['trewscore'].indexOf(shockOnsetx);
		var shockOnsety = jQuery.inArray(shockOnsetx, json['chart_values']['trewscore'])
		verticalMarkings.push(shockMark);
	}

	var plot = $.plot(placeholder, [
		{ data: data, label: "Trewscore", color: "#ca011a"}
	], {
		series: {
			lines: {show: true},
			points: {show: true},
			threshold: [{below: json['trewscore_threshold'], color: "#000000"}]
		},
		legend: {show: false},
		grid: {
			hoverable: true,
			clickable: true,
			markings: verticalMarkings,
			margin: {top: 40,left: 0,bottom: 0,right: 0},
			borderWidth: {top: 0,left: 1,bottom: 1,right: 0}
		},
		crosshair: {mode: "x"},
		yaxis: {
			min: ymin, // should be 0.0
			max: ymax, // sould be 1.0
			ticks: [[0, "0"], [json['trewscore_threshold'], json['trewscore_threshold']]],
			tickColor: "#e64535",
			font: {
				size: 11,
				lineHeight: 13,
				family: "Helvetica, Arial, sans-serif",
				color: "#000"
			}
		},
		xaxis: {
			min: xmin,
			max: xmax,
			mode: "time",
			tickColor: "#EEEEEE",
			timeformat: "%b %e %H:%M",
			timezone: "browser",
			monthNames: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
			font: {
				size: 11,
				lineHeight: 13,
				family: "Helvetica, Arial, sans-serif",
				color: "#000"
			}
		}
	});

	$("#graphdiv").bind("plothover", function (event, pos, item) {

		var str = "(" + pos.x.toFixed(2) + ", " + pos.y.toFixed(2) + ")";

		if (item) {
			var dataIndex = item['dataIndex'];
			var x = item.datapoint[0].toFixed(2),
				y = item.datapoint[1].toFixed(2);

			var features = "<div class='tooltip-header'>\
								<div class='row cf'>\
									<h4 class='name'>TREWScore</h4>\
									<h4 class='value'>" + y + "</h4>\
								</div><div class='row cf time'>\
									<h4 class='name'>Time</h4>\
									<h4 class='value'>" + strToTime(x, true) + "</h4>\
								</div>\
							</div>";
			features += "<div class='row cf'>\
							<h4 class='name'>" + humanReadable(json['chart_values']['tf_1_name'][dataIndex]) + "</h4>\
							<h4 class='value'>" + json['chart_values']['tf_1_value'][dataIndex] + "</h4>\
						</div><div class='row cf'>\
							<h4 class='name'>" + humanReadable(json['chart_values']['tf_2_name'][dataIndex]) + "</h4>\
							<h4 class='value'>" + json['chart_values']['tf_2_value'][dataIndex] + "</h4>\
						</div><div class='row cf'>\
							<h4 class='name'>" + humanReadable(json['chart_values']['tf_3_name'][dataIndex]) + "</h4>\
							<h4 class='value'>" + json['chart_values']['tf_3_value'][dataIndex] + "</h4>\
						</div>";

			$("#tooltip").html(features)
				.css({top: item.pageY+5, left: item.pageX+5})
				.fadeIn(300);
		} else {
			$("#tooltip").hide();
		}
	});

	// Chart Drawing Addistions
	var o = plot.pointOffset({ x: xlast, y: ylast});
	graphTag(plot, xlast, ylast, "Most Recent", "now");
	if (arrivalx && arrivaly) {
		graphTag(plot, arrivalx, arrivaly, "Patient<br/>Arrival", "patient-arrival-graph-tag");
	}
	if (severeOnsetx && severeOnsety) {
		graphTag(plot, severeOnsetx, severeOnsety, "Onset of<br/>Severe Sepsis", "severe-sepsis-graph-tag");
	}
	if (shockOnsetx && shockOnsety) {
		graphTag(plot, shockOnsetx, shockOnsety, "Onset of<br/>Septic Shock", "septic-shock-graph-tag");
	}
	placeholder.append("<div id='threshold' style='left:" + o.left + "px;'>\
			<h3>Septic Shock<br />Risk Threshold</h3>\
			</div>");
}

function graphTag(plot, x, y, text, id) {
	var placeholder = $("#graphdiv");
	var o = plot.pointOffset({ x: x, y: y});
	var xLastTime = new Date(x);
	var minutes = (xLastTime.getMinutes() < 10) ? "0" + xLastTime.getMinutes() : xLastTime.getMinutes();
	placeholder.append("<div id='" + id + "' class='graph-tag' style='left:" + o.left + "px;'>\
			<h3>" + text + "</h3>\
			<h6>" + xLastTime.getHours() + ":" + minutes + "</h6>\
		</div>");
}
