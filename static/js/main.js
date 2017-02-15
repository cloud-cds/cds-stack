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
	this.getMetCriteria = function(slot) {
		var list = [];
		var criteria = (typeof(slot) == "string") ? this.getCriteria(slot) : slot
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
			if (criteria[c]['orveride_user'] != null) {
				list.push(c);
			}
		}
		return list;
	}
};

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
			if (this.criteria[c]['orveride_user'] != null) {
				list.push(c);
			}
		}
		return list;
	}
	this.r = function(json) {
		this.criteria = json['criteria'];
		this.elem.find('h3').text(this.constants['display_name']);
		var isCompleteClass = json['is_met'] ? "complete" : null;
		this.elem.addClass(isCompleteClass);
		for (var c in this.criteria) {
			var component = new criteriaComponent(this.criteria[c], constants['criteria'][c]);
			if (component.isOverridden) {
				this.elem.find('.criteria-overridden').append(component.r());
			} else {
				this.elem.find('.criteria').append(component.r());
			}
		}
		this.elem.find('.num-text').text(json['num_met'] + " criteria met. ");
		if (json['num_met'] == 0) {
			this.elem.find('.edit-btn').addClass('hidden');
		}
		if (this.hasOverridenCriteria().length == 0) {
			this.elem.find('.num-overridden').addClass('hidden');
			this.elem.find('.criteria-overridden').addClass('hidden');
		} else {
			this.elem.find('.num-overridden').text(this.hasOverridenCriteria().length + " overriden criteria");
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
	$('#fake-console').text(window.location);
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
		postBody = {
			q: (getQueryVariable('PATID') === false) ? null : getQueryVariable('PATID'),
			u: (getQueryVariable('EPICUSERID') === false) ? null : getQueryVariable('EPICUSERID'),
			depid: (getQueryVariable('ENCDEPID') === false) ? null : getQueryVariable('ENCDEPID'),
			actionType: (actionType) ? actionType : null,
			action: (actionData) ? actionData : null
		}
		if (getQueryVariable('test') == 'true' || trews.isTest) {
			if (getQueryVariable('console') == 'true')
				console.log(postBody);
			this.test();
			return;
		}
		if (postBody['q'] == null) {
			$('#loading p').html("No Patient Identifier entered. Please restart application or contact trews@opsdx.io<br />" + window.location);
			return;
		}
		$.ajax({
			type: "POST",
			url: this.url,
			data: JSON.stringify(postBody),
			dataType: "json"
		}).done(function(result) {
			$('#loading').addClass('done');
			trews.setData(result);
			controller.refresh();
			// $('#fake-console').text(result);
		}).fail(function(result) {
			endpoints.numTries += 1;
			if (endpoints.numTries > 3) {
				$('#loading p').html("Connection Failed<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews@opsdx.io");
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
			url: 'js/data_example2.json',
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

var controller =  new function() {
	this.clean = function() {
		$('.criteria').html('');
	}
	this.refresh = function() {
		this.clean();
		var globalJson = trews.data;
		severeSepsisComponent.render(globalJson["severe_sepsis"]);
		septicShockComponent.render(globalJson["septic_shock"]);
		workflowsComponent.render(
			globalJson["Antibiotics"],
			globalJson["Blood Culture"],
			globalJson["Fluid"],
			globalJson["Initial Lactate"],
			globalJson["Repeat Lactate"],
			globalJson["Vasopressors"],
			globalJson['chart_data']['severe_sepsis_onset']['timestamp'],
			globalJson['chart_data']['septic_shock_onset']['timestamp']);
		graphComponent.render(globalJson["chart_data"]);
		notifications.render(globalJson['notifications']);
	}
	this.displayJSError = function() {
		$('#loading').removeClass('done');
		$('#loading p').html("Javascript Error<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews@opsdx.io");
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
			this.susCtn.find('.selection').hide();
			this.susCtn.find('.status h4').text(this.sus['value']);
			this.susCtn.find('.status h5').text(
				"by " + this.sus['update_user'] + 
				" at " + timeLapsed(new Date(this.sus['update_time'])));
		}
	}

	this.render = function(json) {
		this.ctn.find('h2').text(severe_sepsis['display_name']);
		var ctnClass = json['is_met'] ? "complete" : "";
		this.ctn.addClass(ctnClass);
		this.sus = json['suspicion_of_infection'];
		this.suspicion(severe_sepsis['suspicion_of_infection']);
		this.sirSlot.r(json['sirs']);
		this.orgSlot.r(json['organ_dysfunction']);
	}
}
var septicShockComponent = new function() {
	this.ctn = $("[data-trews='septicShock']");
	this.tenSlot = new slotComponent(
		$("[data-trews='tension']"), 
		$('#expand-ten'),
		septic_shock['tension']);
	this.fusSlot = new slotComponent(
		$("[data-trews='fusion']"), 
		$('#expand-fus'),
		septic_shock['fusion']);

	this.render = function(json) {
		this.ctn.find('h2').text(septic_shock['display_name']);
		var ctnClass = json['is_met'] ? "complete" : "";
		this.ctn.addClass(ctnClass);
		this.tenSlot.r(json['hypotension']);
		this.fusSlot.r(json['hypoperfusion']);
	}
}

var workflowsComponent = new function() {
	this.sev3Ctn = $("[data-trews='sev3']");
	this.sev6Ctn = $("[data-trews='sev6']");
	this.sep6Ctn = $("[data-trews='sep6']");

	this.clean = function() {
		$("[data-trews='init_lactate'],\
			[data-trews='blood_culture'],\
			[data-trews='antibiotics'],\
			[data-trews='fluid'],\
			[data-trews='re_lactate'],\
			[data-trews='vasopressors']").html(''); //TODO: add focus exam
	}

	this.workflowStatus = function(tag, time) {
		var status = (time == null) ? workflows[tag]['instruction'] : "";
		switch(tag) {
			case 'sev3':
				var offset = 3 * 60 * 60 * 1000;
			case 'sev6':
				var offset = 6 * 60 * 60 * 1000;
			case 'sep6':
				var offset = 6 * 60 * 60 * 1000;
			default:
				var offset = 0;
		}
		if (time + offset < Date.now()) {
			status = "Workflow window over " + timeLapsed(new Date(time + offset));
		} else {
			status = timeRemaining(new Date(time + offset));
		}
		return status;
	}


	this.render = function(aJSON, bJSON, fJSON, iJSON, rJSON, vJSON, severeOnset, shockOnset) {
		// this.clean();
		this.sev3Ctn.find('h2').text(workflows['sev3']['display_name']);
		this.sev6Ctn.find('h2').text(workflows['sev6']['display_name']);
		this.sep6Ctn.find('h2').text(workflows['sep6']['display_name']);
		this.sev3Ctn.find('.card-subtitle').text(this.workflowStatus('sev3', severeOnset));
		this.sev6Ctn.find('.card-subtitle').text(this.workflowStatus('sev6', severeOnset));
		this.sep6Ctn.find('.card-subtitle').text(this.workflowStatus('sep6', shockOnset));

		var iTask = new taskComponent(iJSON, $("[data-trews='init_lactate']"), workflows['init_lactate']);
		var bTask = new taskComponent(bJSON, $("[data-trews='blood_culture']"), workflows['blood_culture']);
		var aTask = new taskComponent(aJSON, $("[data-trews='antibiotics']"), workflows['antibiotics']);
		var fTask = new taskComponent(fJSON, $("[data-trews='fluid']"), workflows['fluid']);

		var rTask = new taskComponent(rJSON, $("[data-trews='re_lactate']"), workflows['repeat_lactate']);

		var vTask = new taskComponent(vJSON, $("[data-trews='vasopressors']"), workflows['vasopressors']);
	}
}

var graphComponent = new function() {
	this.json = {};
	this.xmin = 0;
	this.xmax = 0;
	this.ymin = 0;
	this.ymax = 0;
	$("<div id='tooltip'></div>").appendTo("body");
	this.render = function(json) {
		if (json['chart_values'] == undefined) {
			return;
		}
		this.json = json;
		this.xmin = json['chart_values']['timestamp'][0];
		this.ymin = json['chart_values']['trewscore'][0];
		var max = json['chart_values']['timestamp'][json['chart_values']['timestamp'].length - 1];
		this.xmax = ((max - this.xmin) / 6) + max;
		max = json['chart_values']['trewscore'][json['chart_values']['trewscore'].length - 1];
		this.ymax = ((max - this.ymin) / 6) + max;
		graph(json, this.xmin, this.xmax, this.ymin, this.ymax);
	}
	window.onresize = function() {
		graphComponent.render(graphComponent.json, graphComponent.xmin, graphComponent.xmax);
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
		var units = (elapsed.getSeconds() > 1) ? " secs ago" : " sec ago";
		return elapsed.getSeconds() + units;
	} else if (elapsed < HOUR) {
		var units = (elapsed.getMinutes() > 1) ? " mins ago" : " min ago";
		return elapsed.getMinutes() + units;
	} else if (elapsed < DAY) {
		var units = (elapsed.getHours() > 1) ? " hrs ago" : " hr ago";
		return elapsed.getHours() + units;
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
	return remaining.getHours() + ":" + remaining.getMinutes() + " remaining";
}

/**
 * String to Time
 * Takes a string converts it to a Date object and outputs date/time
 * as such: m/d/y h:m
*/
function strToTime(str) {
	var date = new Date(Number(str));
	var y = date.getFullYear();
	var m = date.getMonth() + 1;
	var d = date.getDate();
	var h = date.getHours() < 10 ? "0" + date.getHours() : date.getHours();
	var min = date.getMinutes() < 10 ? "0" + date.getMinutes() : date.getMinutes();
	return m + "/" + d + "/" + y + " " + h + ":" + min;
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
 * Checks if object is empty
*/
function isEmpty(object) {
	for(var i in object) {
		return true;
	}
	return false;
}

/**
 * Criteria Component
 * @param JSON String
 * @return {String} html for a specific criteria
 */
var criteriaComponent = function(c, constants) {
	this.isOverridden = false;

	if (c['is_met']) {
		this.classComplete = " met";
		var lapsed = timeLapsed(new Date(c['measurement_time']));
		this.status = "Criteria met " + lapsed + " with a value of <span class='value'>" + c['value'] + "</span>";
	} else {
		if (c['orveride_user'] != null) {
			this.classComplete = " unmet";
			this.isOverridden = true;
			var cLapsed = timeLapsed(new Date(c['measurement_time']));
			var oLapsed = timeLapsed(new Date(c['override_time']));
			this.status = "Criteria met " + cLapsed + " with a value of <span class='value'>" + c['value'] + "</span>";
			this.status += "<br />Overridden by " + c['orveride_user'] + " " + oLapsed;
		} else {
			this.classComplete = " hidden unmet";
			this.status = "";
		}
	}
	this.html = "<div class='status" + this.classComplete + "'>\
					<h4>" + constants['criteria_display_name'] + "</h4>\
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
	// var actions = "<div class='actions cf'>\
	// 					<a class='order'>Place Order</a>\
	// 					<a class='notIn'>Not Indicated</a>\
	// 				</div>";
	// elem.append(actions);
	// elem.find('.order').click(function() {

	// });
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
	this.getAction = function(value) {
		var action = {
			"actionType": "override",
			"card": CONSTANTS[this.d.attr('data-trews')],
			"criteria": this.d.attr('data-trews'),
			"value": value
		}
		return action;
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
		var metCriteriaIndices = trews.getMetCriteria(field);
		for (var i in EDIT[field]) {
			if (jQuery.inArray(i, metCriteriaIndices) > -1) {
				var s = $('<h5 class="dropdown-link"></h5>').text(EDIT[field][i]);
				this.ctn.append(s);
			}
		}
		$('.dropdown-link').click({index: metCriteriaIndices}, function(e) {
			var action = dropdown.getAction($(this).text());
			overrideModal.launch(action, e.data.index);
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
	this.init = function() {
		this.om.append(this.ctn);
	}
	this.modalView = function(data) {
		var html = "<h3>" + data['header'] + "</h3>";
		html += "<p>Define a new acceptable range.  The criteria will be met once the patient's " + data['name'] + " falls out of the new range.</p>";
		html += "<div><span class='slider-numbers' data-trews='" + data['id'] + "'></span></div>"
		html += "<div class='slider-range' data-trews='" + data['id'] + "'></div>";
		// html += "<p>or define a lockout period.  During this lockout period the criteria will not be met.  The criteria will be reevaluated after once the lockout period ends.</p>";
		// html += "<input class='override-lockout' data-trews='" + data['id'] + "' type='num'>";
		return html;
	}
	this.makeSliders = function(data) {
		// console.log(data);
		if (data['range'] === "true") {
			$(".slider-range[data-trews='" + data['id'] + "']").slider({
				range: data['range'],
				min: data['minAbsolute'],
				max: data['maxAbsolute'],
				step: data['step'],
				values: data['values'],
				slide: function( event, ui ) {
					$(".slider-numbers[data-trews='" + data['id'] + "']").text(ui.values[0] + " - " + ui.values[1]);
				}
			});
			$(".slider-numbers[data-trews='" + data['id'] + "']").text($(".slider-range[data-trews='" + data['id'] + "']").slider("values",0) + " - " + $(".slider-range[data-trews='" + data['id'] + "']").slider("values",1));
		} else {
			if (data['range'] === "min") {
				slideFunction = function(event, ui) {
					$(".slider-numbers[data-trews='" + data['id'] + "']").text(data['minAbsolute'] + " - " + ui.value);
				}
			} else {
				slideFunction = function(event, ui) {
					$(".slider-numbers[data-trews='" + data['id'] + "']").text(ui.value + " - " + data['maxAbsolute']);
				}
			}
			$(".slider-range[data-trews='" + data['id'] + "']").slider({
				range: data['range'],
				min: data['minAbsolute'],
				max: data['maxAbsolute'],
				step: data['step'],
				value: data['value'],
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
		save.unbind();
		save.click(function() {
			var sliders = $('.slider-range');
			var postData = [];
			for (var i = 0; i < sliders.length; i++) {
				var criteria = sliders[i].getAttribute('data-trews');
				if ($(".slider-range[data-trews='" + criteria + "']").slider("values").length == 0) {
					var value = $(".slider-range[data-trews='" + criteria + "']").slider("value");
					var values = null;
				} else {
					var value = null;
					var values = $(".slider-range[data-trews='" + criteria + "']").slider("values");
				}
				var criteriaOverride = {
					"criteria": criteria,
					"value": value,
					"values": values
				}
				postData.push(criteriaOverride);
			}
			endpoints.getPatientData("override", postData);
			overrideModal.om.fadeOut(30);
		});
		cancel.unbind();
		cancel.click(function() {
			overrideModal.om.fadeOut(30);
		});
	}
	this.launch = function(action, index) {
		this.ctn.html("");
		if (!Modernizr.opacity) {
			this.om.addClass('no-opacity');
		}
		var overrideModal = STATIC[action['card']][action['criteria']]['criteria'][Number(index[0])]['overrideModal'];
		for (var i = 0; i < overrideModal.length; i++) {
			this.ctn.append(this.modalView(overrideModal[i]));
			this.makeSliders(overrideModal[i]);
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
			notifications.n.fadeIn(30);
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
			subtext.append('<p>' + timeLapsed(new Date(data[i]['time_stamp'])) + '</p>');
			var readLink = $("<a data-trews='" + data[i]['id'] + "'></a>");
			readLink.unbind();
			if (data[i]['read']) {
				// notif.addClass('read');  // holding off this feature for 2/15 launch
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
			// subtext.append(readLink); // holding off this feature for 2/15 launch
			notif.append(subtext);
			this.n.prepend(notif);
		}
		// this.nav.find('.num').text(numUnread);  // holding off this feature for 2/15 launch
		this.nav.find('.num').text(data.length);
		if (data.length > 1) {
			this.nav.find('.text').text('Notifications');
		} else {
			this.nav.find('.text').text('Notification');
		}
	}
}

function graph(json, xmin, xmax, ymin, ymax) {
	var graphWidth = Math.floor($('#graph-wrapper').width()) - 60;
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

	var plot = $.plot(placeholder, [
		{ data: data, label: "Trewscore", color: "#ca011a"}
	], {
		series: {
			lines: {show: true},
			points: {show: true},
			threshold: [{below: .5,	color: "#000000"}]
		},
		curvedLines: {apply: true},
		legend: {show: false},
		grid: {
			hoverable: true,
			clickable: true,
			markings: [{color: "#555",lineWidth: 1,xaxis: {from: xlast,to: xlast}}],
			margin: {top: 40,left: 0,bottom: 0,right: 0}, 
			borderWidth: {top: 0,left: 1,bottom: 1,right: 0}
		},
		crosshair: {mode: "x"},
		yaxis: {
			min: ymin, // should be 0.0
			max: ymax, // sould be 1.0
			ticks: [[0, "0"], [0.5, "0.5"]],
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
			timeformat: "%H:%M",
			timezone: "browser",
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

			var features = "<div class='row cf'>\
								<h4 class='name'>TREWScore</h4>\
								<h4 class='value'>" + y + "</h4>\
							</div><div class='row cf'>\
								<h4 class='name'>Time</h4>\
								<h4 class='value'>" + strToTime(x) + "</h4>\
							</div>";
			features += "<div class='row cf'>\
							<h4 class='name'>" + json['chart_values']['tf_1_name'][dataIndex] + "</h4>\
							<h4 class='value'>" + json['chart_values']['tf_1_value'][dataIndex] + "</h4>\
						</div><div class='row cf'>\
							<h4 class='name'>" + json['chart_values']['tf_2_name'][dataIndex] + "</h4>\
							<h4 class='value'>" + json['chart_values']['tf_2_value'][dataIndex] + "</h4>\
						</div><div class='row cf'>\
							<h4 class='name'>" + json['chart_values']['tf_3_name'][dataIndex] + "</h4>\
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
	var xLastTime = new Date(xlast);
	placeholder.append("<div id='now' style='left:" + o.left + "px;'>\
			<h3>Now</h3>\
			<h6>" + xLastTime.getHours() + ":" + xLastTime.getMinutes() + "</h6>\
			</div>");
	placeholder.append("<div id='threshold' style='left:" + o.left + "px;'>\
			<h3>Septic Shock<br />Risk Threshold</h3>\
			</div>");

}
