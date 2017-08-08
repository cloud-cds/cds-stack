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

if (!String.prototype.startsWith) {
    String.prototype.startsWith = function(searchString, position){
      position = position || 0;
      return this.substr(position, searchString.length) === searchString;
  };
}

(function($) {
    $.fn.hasScrollBar = function() {
        var e = this.get(0);
        return {
            vertical: e.scrollHeight > e.clientHeight,
            horizontal: e.scrollWidth > e.clientWidth
        };
    }
})(jQuery);

/**
 * Helpers.
 */
function appendToConsole(txt) {
  var consoleText = $('#fake-console').html();
  if ( consoleText.length > 16384 ) { consoleText = ''; }
  consoleText += '<br>' + txt;
  $('#fake-console').html(consoleText);
}

function logSuspicion(tag) {
  var txt = '';
  var logDate = new Date();
  if ( trews.data != null && trews.data.severe_sepsis != null && trews.data.severe_sepsis.suspicion_of_infection != null ) {
    var fieldDate = new Date(trews.data.severe_sepsis.suspicion_of_infection.update_time*1000);
    txt = tag + ' ' + trews.data.severe_sepsis.suspicion_of_infection.name + ' ' + fieldDate.toISOString() + ' ' + logDate.toISOString();
  } else {
    txt = tag + ' null null ' + logDate.toISOString();
  }
  appendToConsole(txt);
}

function orderStatusCompleted(objWithStatus) {
  var orderNA = objWithStatus['status'] == null ?
                  false : objWithStatus['status'].startsWith('Clinically Inappropriate');

  return objWithStatus['status'] == 'Completed'
          || objWithStatus['status'] == 'Not Indicated'
          || orderNA;
}


/**
 * Globals.
 */
var release = $('body').attr('release');

/**
 * Epic 2017 AGL Listener.
 */
var epicToken = null;
var lastAction = null;

function Listener(event) {
  for (var type in event.data) {
    var payload = event.data[type]
    switch(type) {
      case "token":
        epicToken = payload;
        appendToConsole('Epic handshake done: ' + encodeURI(epicToken));
        break;

      case "error":
        var url = (window.location.hostname.indexOf("localhost") > -1) ?
                    "http://localhost:8000/api" :
                    window.location.protocol + "//" + window.location.hostname + "/api";
        var ts = new Date();
        timer.log(url, ts, ts, 'Epic error: ' + JSON.stringify(payload));
        appendToConsole('Epic error: ' + JSON.stringify(payload));
        break;

      case "features":
        appendToConsole('features:');
        for (var feature in payload) {
          appendToConsole('  ' + feature);
        }
        break;

      case "state":
        appendToConsole('state: ' + JSON.stringify(payload));
        break;

      case "actionExecuted":
        var actionTxt = lastAction == null ? '<unknown>' : lastAction;
        appendToConsole('actionExecuted: ' + actionTxt + ' ' + payload.toString());
        break;

      default:
        appendToConsole('unhandled event: ' + JSON.stringify(event));
        break;
    }
  }
}

/**
 * Window callbacks.
 */
window.onload = function() {
  // Handshake with Epic 2017 AGL.
  lastAction = 'handshake';
  window.addEventListener("message", Listener, false);
  window.parent.postMessage({'action': 'Epic.Clinical.Informatics.Web.InitiateHandshake'}, '*');

  timer.init();
  endpoints.getPatientData();
  dropdown.init();
  overrideModal.init();
  notifications.init();
  activity.init();
  toolbar.init()
  dataRefresher.init();
  notificationRefresher.init();
  deterioration.init();
  $('#fake-console').text(window.location);
  $('#fake-console').hide();
  $('#show-console').click(function() {
    $('#fake-console').toggle();
  })
  // Bugsnag.notify("ErrorName", "Test Error");
};

window.onerror = function(error, url, line) {
  controller.sendLog({acc:'error', data:'ERR:'+error+' URL:'+url+' L:'+line}, true);
};

checkIfOrdered = null; // Global bool to flip when clicking "place order"
window.onresize = function() {
  graphComponent.render(trews.data.chart_data,
                        (trews.data.severe_sepsis != null ? trews.data.severe_sepsis.onset_time : null),
                        (trews.data.septic_shock != null ? trews.data.septic_shock.onset_time : null),
                        graphComponent.xmin, graphComponent.xmax);

  if ( checkIfOrdered != null) {
    endpoints.getPatientData('place_order', {'actionName': checkIfOrdered});
    checkIfOrdered = null;
  }
}


/**
 * State Tree, Maintains most up to date app information
 * from server.  Source of Truth
*/
var trews = new function() {
  this.data = {};
  this.isTest = false;
  this.setData = function(data) {
    if (this.data != null) {
      data['antibiotics_details'] = this.data['antibiotics_details'];
    }
    this.data = data;
  }
  this.setNotifications = function(notifications) {
    if (this.data) {
      this.data['notifications'] = notifications;
    } else {
      this.data = {'notifications': notifications}
    }
  }
  this.setAntibiotics = function(antibiotics) {
    if (this.data) {
      this.data['antibiotics_details'] = antibiotics;
    } else {
      this.data = {'antibiotics_details': antibiotics}
    }
  }
  this.getCriteria = function(slot) {
    switch(slot) {
      case 'sirs':
        return this.data['severe_sepsis']['sirs']['criteria'];
      case 'org':
        return this.data['severe_sepsis']['organ_dysfunction']['criteria'];
      case 'organ_dysfunction':
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
      if (criteria[c]['is_met'] === true) {
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
  this.orderIsDone = function(order_name) {
    if (this.data[order_name]) {
      return orderStatusCompleted(this.data[order_name]);
    }
    return false;
  }
  this.getIncompleteSevereSepsis3hr = function() {
    return (   ( this.orderIsDone('antibiotics_order')       ? 0 : 1 )
             + ( this.orderIsDone('initial_lactate_order')   ? 0 : 1 )
             + ( this.orderIsDone('blood_culture_order')     ? 0 : 1 )
             + ( this.orderIsDone('crystalloid_fluid_order') ? 0 : 1 )
           );
  }
  this.getIncompleteSevereSepsis6hr = function() {
    return ( this.orderIsDone('repeat_lactate_order') ? 0 : 1 );
  }
  this.getIncompleteSepticShock = function() {
    return ( this.orderIsDone('vasopressors_order') ? 0 : 1 );
  }
};


/**
 * Polling timer loops.
 * dataRefresher: a timer for periodically retrieving the full trews data object.
 * notificationRefresher: a timer for periodically retrieving the notifications list.
 */

var dataRefresher = new function() {
  this.refreshPeriod = 30000;
  this.refreshTimer = null;
  this.init = function() {
    this.poll(this);
  }
  this.poll = function(obj) {
    endpoints.getPatientData();
    obj.refreshTimer = window.setTimeout(function() { obj.poll(obj); }, obj.refreshPeriod);
  }
  this.terminate = function() {
    if (this.refreshTimer) {
      window.clearTimeout(this.refreshTimer)
      this.refreshTimer = null
    }
  }
}

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
 * Endpoints Object handles sending and receing post requests to server.
 * Handles different connectivity states Inlcuding normal use, testing
 * and improper connection.
*/
var endpoints = new function() {
  this.url = (window.location.hostname.indexOf("localhost") > -1) ?
    "http://localhost:8000/api" :
    window.location.protocol + "//" + window.location.hostname + "/api";
  this.numTries = 1;
  this.getPatientData = function(actionType, actionData, toolbarButton) {
    postBody = {
      q: (getQueryVariable('PATID') === false) ? null : getQueryVariable('PATID'),
      u: (getQueryVariable('USERID') === false) ? null : cleanUserId(getQueryVariable('USERID')),
      depid: (getQueryVariable('DEP') === false) ? null : getQueryVariable('DEP'),
      csn: (getQueryVariable('CSN') === false) ? null : getQueryVariable('CSN'),
      loc: (getQueryVariable('LOC') === false) ? null : getQueryVariable('LOC'),
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
    // Location filtering: JHH/BMC/HCGH only for now.
    if ( postBody['loc'] == null ||
          !(postBody['loc'].startsWith('1101')
            || postBody['loc'].startsWith('1102')
            || postBody['loc'].startsWith('1103')) )
    {
      $('#loading p').html(
          "TREWS is in beta testing, and is only available at the Johns Hopkins Hopsital, Bayview Medical Center and Howard County General Hospital.<br/>"
          + "Please contact trews-jhu@opsdx.io for more information on availability at your location.<br/>");
      return;
    }
    // Ensure a valid Patient ID.
    if (postBody['q'] == null) {
      $('#loading p').html("No Patient Identifier entered. Please restart application or contact trews-jhu@opsdx.io<br/>" + window.location);
      return;
    }
    $.ajax({
      type: "POST",
      url: this.url,
      data: JSON.stringify(postBody),
      dataType: "json",
      start_time: new Date().getTime()
    }).done(function(result) {
      $('body').removeClass('waiting');
      $('#loading').html('');
      $('#loading').addClass('done');
      if ( toolbarButton ) { toolbarButton.removeClass('loading'); }
      if ( result.hasOwnProperty('trewsData') ) {
        $('#loading').removeClass('waiting').spin(false); // Remove any spinner from the page
        trews.setData(result.trewsData);
        if ( trews.data && trews.data['refresh_time'] != null ) { // Update the Epic refresh time.
          var refreshMsg = 'Last refreshed from Epic at ' + strToTime(new Date(trews.data['refresh_time']*1000), true, true) + '.';
          $('h1 #header-refresh-time').text(refreshMsg);
        }
        logSuspicion('set'); // Suspicion debugging.
        controller.refresh();
        controller.refreshOrderDetails('antibiotics-details'); // Refresh order details due to clinically inappropriate updates.
        deterioration.dirty = false
      } else if ( result.hasOwnProperty('notifications') ) {
        trews.setNotifications(result.notifications);
        controller.refreshNotifications();
      } else if ( result.hasOwnProperty('getAntibioticsResult') ) {
        trews.setAntibiotics(result.getAntibioticsResult);
        controller.refreshOrderDetails('antibiotics-details');
      }
      timer.log(this.url, this.start_time, new Date().getTime(), 'success')
    }).fail(function(result) {
      $('body').removeClass('waiting');
      $('#loading').removeClass('waiting').spin(false); // Remove any spinner from the page
      if ( toolbarButton ) { toolbarButton.removeClass('loading'); }
      if (result.status == 400) {
        $('#loading p').html(result.responseJSON['message'] + ".<br/>  Connection Failed<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews-jhu@opsdx.io");
        $('#test-data').click(function() {
          endpoints.test();
        });
        return;
      }
      endpoints.numTries += 1;
      if (endpoints.numTries > 3) {
        $('#loading p').html("Connection Failed<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews-jhu@opsdx.io");
        $('#test-data').click(function() {
          endpoints.test();
        });
        $('#see-blank').click(function() {
          $('#loading').html('');
          $('#loading').addClass('done');
        });
      } else {
        $('#loading p').text("Connection Failed. Retrying...(" + endpoints.numTries + ")");
        endpoints.getPatientData();
      }
      timer.log(this.url, this.start_time, new Date().getTime(), 'error: ' + endpoints.numTries + ' tries')
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
        $('#loading').html('');
        $('#loading').addClass('done');
        trews.setData(result.trewsData);
        controller.refresh();
      }
    });
  }
}

/**
 * TREWS View Controller.
 *
 * A data-driven view controller that refreshes views whenever trews
 * data and notifications have successfully been retrieved from the endpoint.
 */
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
      globalJson['severe_sepsis']['onset_time'],
      globalJson['septic_shock']['onset_time']);
    graphComponent.refresh(globalJson["chart_data"]);
    notifications.render(globalJson['notifications']);
    activity.render(globalJson['auditlist']);
    toolbar.render(globalJson["severe_sepsis"]);
    deterioration.render(globalJson['deterioration_feedback']);

    // Suspicion debugging.
    logSuspicion('rfs');
  }
  this.refreshNotifications = function() {
    var globalJson = trews.data;
    notifications.render(globalJson['notifications']);
  }

  // TODO: handle details for every order type.
  this.refreshOrderDetails = function(order_details_type) {
    var orderType = order_details_type == 'antibiotics-details' ? 'antibiotics_order' : null;
    if ( order_details_type == null || orderType == null ) {
      throw ('Failed to refresh order details');
    }

    var unique_order_elems = [];

    // Add clincially inappropriate as a status.
    var naMsg = '';
    var naPrefix = 'Clinically Inappropriate';
    if ( trews.data && trews.data[orderType]
          && trews.data[orderType]['status'] != null
          && trews.data[orderType]['status'].startsWith(naPrefix)
        )
    {
      naMsg = naPrefix;
      if (trews.data[orderType]['status'].length > naPrefix.length + 1) {
        naMsg += ': ' + trews.data[orderType]['status'].substr(naPrefix.length + 1);
      }
      unique_order_elems.push(naMsg)
    }

    // Add individual antibiotics as a status.
    if (trews.data.antibiotics_details != null) {
      for (var i in trews.data.antibiotics_details) {
        var order_elem = trews.data.antibiotics_details[i].order_name;
        if (unique_order_elems.indexOf(order_elem) === -1) {
          unique_order_elems.push(order_elem);
        }
      }
    }

    var detailsLink = $("span[data-trews='" + order_details_type + "']").find('.inspect');
    var detailsCtn = $(".order-details-content[data-trews='" + order_details_type + "']").find(".status");
    detailsComponent = new orderDetailsComponent(unique_order_elems);

    // Sync content state with link state.
    if ( detailsLink.hasClass('unhidden') ) {
      detailsCtn.removeClass('hidden').addClass('unhidden');
    } else {
      detailsCtn.removeClass('unhidden').addClass('hidden');
    }
    detailsCtn.html(detailsComponent.r());
  }

  this.displayJSError = function() {
    dataRefresher.terminate();
    notificationRefresher.terminate();
    $('#loading').removeClass('done');
    $('#loading p').html("Javascript Error<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews-jhu@opsdx.io");
    $('#test-data').click(function() {
      endpoints.test();
    });
    $('#see-blank').click(function() {
      $('#loading').html('');
      $('#loading').addClass('done');
    });
  }
  this.sendLog = function(json, isError) {
    $.ajax({
      type: "POST",
      url: "log",
      data: JSON.stringify(json),
      dataType: "json"
    }).done(function(result) {
      if (isError)
        controller.displayJSError();
    }).fail(function(result) {
      if (isError)
        controller.displayJSError();
    });
  }
  this.sendFeedback = function(json) {
    $.ajax({
      type: "POST",
      url: "feedback",
      data: JSON.stringify(json),
      dataType: "json"
    }).done(function(result) {
      toolbar.feedbackSuccess();
    }).fail(function(result) {
      toolbar.feedbackError();
    });
  }
}

var timer = new function() {
  this.refreshPeriod = 60000;
  this.refreshTimer = null;
  this.buffer = []
  this.init = function() {
    this.sendBuffer(this);
    if (window.addEventListener) {
      window.addEventListener("beforeunload", function (e) {
        if (timer.buffer.length > 0) {
          controller.sendLog({buffer: timer.buffer}, false)
        }
      });
    } else {
      window.attachEvent("beforeunload", function (e) {
        if (timer.buffer.length > 0) {
          controller.sendLog({buffer: timer.buffer}, false)
        }
      });
    }
  }
  this.log = function(url, start, end, status) {
    var postBody = {
      "currentUrl": window.location.href,
      "destinationURL": url,
      "start_time": start,
      "end_time": end,
      "status": status
    }
    this.buffer.push(postBody)
  }
  this.sendBuffer = function(obj) {
    // checks every minute if buffer has times to send back
    if (timer.buffer.length > 0) {
      controller.sendLog({buffer: timer.buffer}, false)
      timer.buffer = []
    }
    obj.refreshTimer = window.setTimeout(function() { obj.sendBuffer(obj); }, obj.refreshPeriod);
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
      var component = new criteriaComponent(this.criteria[c], constants['criteria'][c], constants.key, this.link.hasClass('hidden'));
      if (component.isOverridden) {
        this.elem.find('.criteria-overridden').append(component.r());
      } else {
        this.elem.find('.criteria').append(component.r());
      }
    }
    this.elem.find('.num-text').text(json['num_met'] + " criteria met. ");
    // this.elem.find('.edit-btn').addClass('hidden'); // Yanif: (REENABLED; Temporarily disabling overrides).
    /*
    if (json['num_met'] == 0) {
      this.elem.find('.edit-btn').addClass('hidden');
    }
    */
    if (this.hasOverridenCriteria().length == 0) {
      this.elem.find('.num-overridden').addClass('hidden');
      this.elem.find('.criteria-overridden').addClass('hidden');
    } else {
      this.elem.find('.num-overridden').removeClass('hidden');
      this.elem.find('.criteria-overridden').removeClass('hidden');
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
          //  opacity: 1
          // }, 300 );
        }
      } else {
        e.data.elem.find('.status.unhidden').removeClass('unhidden').addClass('hidden');
        $(this).text('see all').addClass('hidden');
        this.criteriaHidden = true;
        if (!Modernizr.csstransitions) {
          // TODO figure out animations on ie8
          // $("[data-trews='sir']").find('.status.unhidden').animate({
          //  opacity: 0
          // }, 300 );
        }
      }
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

  // Returns the class to attach to the SOI slot (to highlight it).
  // Returns null if no highlighting is to be performed.
  this.highlightSuspicionClass = function() {
    var renderTs = Date.now();
    var active205 = false;
    var active300 = false;

    if ( trews.data != null && trews.data.notifications != null ) {
      for (var i = 0; i < trews.data.notifications.length; i++) {
        // Skip notifications scheduled in the future, as part of the Dashan event queue.
        var notifTs = new Date(trews.data.notifications[i]['timestamp'] * 1000);
        if (notifTs > renderTs) { continue; }
        if ( trews.data.notifications[i]['alert_code'] == '205' ) { active205 = true; }
        if ( trews.data.notifications[i]['alert_code'] == '300' ) { active300 = true; }
      }
    }

    appendToConsole('soi-highlight 205:' + active205.toString() + ' 300:' + active300.toString());
    if ( active300 ) {
      return active205 ? 'highlight-expired' : 'highlight-unexpired';
    }
    return null;
  }

  this.suspicion = function(json) {
    this.susCtn.find('h3').text(json['display_name']);
    this.susCtn.removeClass('complete');
    if (this.sus['value'] == null) {
      var highlightCls = this.highlightSuspicionClass();
      if ( highlightCls != null ) {
        this.susCtn.addClass(highlightCls);
        this.susCtn.find('.status').show();
        this.susCtn.find('.status h4').text('Please enter a suspicion of infection');
      } else {
        this.susCtn.removeClass('highlight-expired highlight-unexpired');
        this.susCtn.find('.status').hide();
      }
      this.susCtn.find('.status h5').html('');
    } else {
      this.susCtn.removeClass('highlight-expired highlight-unexpired');
      if (this.sus['value'] != 'No Infection') {
        this.susCtn.addClass('complete');
      }
      this.susCtn.find('.status').show();
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

    if (trews.data['deactivated']) {
      this.ctn.addClass('inactive');
    } else {
      this.ctn.removeClass('inactive');
    }
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
      fnoteBtnText = "Reset";
    } else {
      fnoteBtnText = "Not Indicated";
    }

    if ( fnoteBtnText ) {
      this.fnoteBtn.text(fnoteBtnText);
      var action = {
        "actionName": 'crystalloid_fluid'
      };

      if ( fnoteBtnText == 'Not Indicated' ) {
        action['value'] = [{'text': 'Not Indicated'}];
      } else {
        action['clear'] = true;
      }
      this.fnoteBtn.click(function() {
        endpoints.getPatientData("override", action);
      });
    } else {
      this.fnoteBtn.hide();
    }

    if (trews.data['deactivated'] || !severeSepsis) {
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
  this.orderBtns = $('.place-order-btn');
  this.orderNABtns = $('.orderNA');
  this.tasks = [];

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
      var order = $(this).attr('data-trews');
      var key = $(this).attr('order-key');

      // Track order placed action.
      lastAction = order;
      checkIfOrdered = order;

      if ( release == 'epic2017' ) {
        // TODO: should we remove specific orders before posting, to implement replacement order semantics?
        appendToConsole('EPIC 2017 ORDER: ' + order + ' KEY: ' + key);
        if ( window.parent != null ) {
          window.parent.postMessage({
            'token': epicToken,
            'action': 'Epic.Clinical.Informatics.Web.PostOrder',
            'args': { 'OrderKey': key }
          }, '*');
        } else {
          appendToConsole('Skipping order (null parent)');
        }
      }
      else {
        var txt = $(this).get()[0].innerHTML;
        var anc = $(this).find('a').first().get()[0];
        if ( anc != null ) { txt += '\nANCHOR:' + anc.innerHTML; }
        appendToConsole(txt);

        checkIfOrdered = null;
        if ( anc != null ) {
          anc.click();
          checkIfOrdered = $(this).attr('data-trews');
        }
      }
    });

    this.orderNABtns.unbind();
    this.orderNABtns.click(function(e) {
      e.stopPropagation();
      var naDropdown = $('#order-inappropriate-dropdown');
      var orderType = $(this).attr('data-trews');

      // Hide everything else.
      notifications.n.fadeOut(300);
      activity.a.fadeOut(300);
      dropdown.d.fadeOut(300);
      deterioration.sendOff();
      $('.order-dropdown').fadeOut(300);

      // Set the active order.
      naDropdown.find('span').attr('data-trews', orderType);

      // Retrieve and set any existing reason.
      var orderNAPrefix = 'Clinically Inappropriate:';
      var orderNAValue = '';
      if ( trews.data && trews.data[orderType] && trews.data[orderType].status) {
        if ( trews.data[orderType].status.startsWith(orderNAPrefix) ) {
          orderNAValue = trews.data[orderType].status.substr(orderNAPrefix.length);
        }
      }
      $('#order-inappropriate-dropdown input').val(orderNAValue);

      var parent_card = $(this).closest('.card');
      if ( parent_card != null ) {
        naDropdown.css({
          top: $(this).offset().top + $(this).height() + 7,
          left: parent_card.offset().left + parseInt(parent_card.css('padding-left'), 10)
        }).fadeIn(30);
      } else {
        throw ('Failed to place ' + orderType.toUpperCase() + ' order.');
      }
    });
  }

  this.workflowStatus = function(tag, time, lastOrderTime, completed) {
    if (time == null) {
      return workflows[tag]['not_yet'];
    }

    var status = workflows[tag]['instruction'];
    if ( completed ) {
      var completedDate = new Date(lastOrderTime * 1000);
      status = "Workflow window completed <span title='" + strToTime(completedDate) + "'>" + timeLapsed(completedDate) + "</span>";
      return status;
    }

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
      status = "<span class='workflow-expired'>Workflow window expired <span title='" + strToTime(wfDate) + "'>" + timeLapsed(wfDate) + "</span></span>";
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

    if (trews.data['deactivated'] || severeOnset == null) {
      this.sev3Ctn.addClass('inactive');
      this.sev6Ctn.addClass('inactive');
    } else {
      this.sev3Ctn.removeClass('inactive');
      this.sev6Ctn.removeClass('inactive');
    }

    if (trews.data['deactivated'] || shockOnset == null) {
      this.sep6Ctn.addClass('inactive');
    } else {
      this.sep6Ctn.removeClass('inactive');
    }

    var sev3LastOrder = Math.max(iJSON['time'], bJSON['time'], aJSON['time'], fJSON['time']);
    var sev3Complete = orderStatusCompleted(iJSON) &&
                       orderStatusCompleted(bJSON) &&
                       orderStatusCompleted(aJSON) &&
                       orderStatusCompleted(fJSON);

    var sev6LastOrder = Math.max(sev3LastOrder, rJSON['time'])
    var sev6Complete = sev3Complete && orderStatusCompleted(rJSON);

    var shk6LastOrder = Math.max(sev6LastOrder, vJSON['time'])
    var shk6Complete = sev6Complete && orderStatusCompleted(vJSON);

    this.sev3Ctn.find('.card-subtitle').html(this.workflowStatus('sev3', severeOnset, sev3LastOrder, sev3Complete));
    this.sev6Ctn.find('.card-subtitle').html(this.workflowStatus('sev6', severeOnset, sev6LastOrder, sev6Complete));
    this.sep6Ctn.find('.card-subtitle').html(this.workflowStatus('sep6', shockOnset, shk6LastOrder, shk6Complete));

    this.tasks = [
      new taskComponent(iJSON, $("[data-trews='init_lactate']"), workflows['init_lactate'], null),
      new taskComponent(bJSON, $("[data-trews='blood_culture']"), workflows['blood_culture'], null),
      new taskComponent(aJSON, $("[data-trews='antibiotics']"), workflows['antibiotics'], null /*doseLimits['antibiotics']*/),
      new taskComponent(fJSON, $("[data-trews='fluid']"), workflows['fluid'], null /*doseLimits['fluid']*/),
      new taskComponent(rJSON, $("[data-trews='re_lactate']"), workflows['repeat_lactate'], null),
      new taskComponent(vJSON, $("[data-trews='vasopressors']"), workflows['vasopressors'], null /*doseLimits['vasopressors']*/)
    ];

    this.makeButtons();
  }

  // Initialize order inappropriate dropdown.
  var slotWidth = parseInt($('.card[data-trews="sev3"] .slot').css('width'), 10);
  var orderNAMaxWidth = parseInt($('#order-inappropriate-dropdown').css('max-width'), 10);
  if ( slotWidth < orderNAMaxWidth ) {
    var newWidth = slotWidth - ($('#right-column').hasScrollBar().vertical ? 15 : 0);
    var newCss = {'max-width': newWidth + 'px'};
    if ( newWidth < 300 ) {
      newCss['font-size'] = '10px';
      $('#order-inappropriate-dropdown input').css({'font-size': '10px'});
    }
    $('#order-inappropriate-dropdown').css(newCss);
  }

  $('#order-inappropriate-dropdown input').click(function(e) {
    e.stopPropagation();
  });

  $('#order-inappropriate-dropdown span[data-action="submit"]').click(function(e) {
    e.stopPropagation();
    var orderType = $(this).attr('data-trews');
    if ( orderType != null ) {
      $('#loading').addClass('waiting').spin(); // Add spinner to page
      endpoints.getPatientData('order_inappropriate', {
        'actionName': orderType,
        'reason': $('#order-inappropriate-dropdown input').val()
      });
    }
    $('#order-inappropriate-dropdown').fadeOut(300);
  });

  $('#order-inappropriate-dropdown span[data-action="reset"]').click(function(e) {
    e.stopPropagation();
    var orderType = $(this).attr('data-trews')
    var orderNAPrefix = 'Clinically Inappropriate';

    // Check that the current status is clinically inappropriate,
    // to ensure that we do not override an Ordered/Completed status.
    if ( trews.data && trews.data[orderType] && trews.data[orderType].status ) {
      if ( trews.data[orderType].status.startsWith(orderNAPrefix) ) {
        $('#loading').addClass('waiting').spin(); // Add spinner to page
        endpoints.getPatientData('override', {'actionName': orderType, 'clear': true});
      }
    }
    $('#order-inappropriate-dropdown').fadeOut(300);
  });
}

/**
 * TREWS Chart.
 * A component and supporting functions representing the TREWScore time series plot.
 */
var graphComponent = new function() {
  this.is30 = true;
  this.xmin = 0;
  this.xmax = 0;
  this.ymin = 0;
  this.ymax = 0;
  $("<div id='tooltip'></div>").appendTo("body");
  this.refresh = function(json) {
    this.is30 = true;
    this.render(json, trews.data.severe_sepsis.onset_time, trews.data.septic_shock.onset_time);
  }
  this.render = function(json, severeOnset, shockOnset) {
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

    var trewsDataLength = json['chart_values']['trewscore'].length;
    for (var i = 0; i < trewsDataLength; i += 1) {
      json['chart_values']['trewscore'][i] = Math.min(1.0, Math.max(0.0, json['chart_values']['trewscore'][i])); // Clamp chart values to 0-1 range.
    }

    if (json.chart_values.timestamp.length != 0) {
      this.xmin = json['chart_values']['timestamp'][0];
      var max = json['chart_values']['timestamp'][json['chart_values']['timestamp'].length - 1];
      this.xmax = ((max - this.xmin) / 6) + max;
    } else {
      if (json.patient_arrival != null) {
        this.xmin = json.patient_arrival.timestamp * 1000;
        this.xmax = ((Date.now() - this.xmin) / 6) + Date.now();
      } else {
        this.xmin = (Date.now() * 1000) - (6 * 60 * 60 * 1000)
        this.xmax = (Date.now() * 1000) + (6 * 60 * 60 * 1000)
      }
    }
    // this.ymin = 0;//json['chart_values']['trewscore'][0];
    // max = json['chart_values']['trewscore'][json['chart_values']['trewscore'].length - 1];
    // this.ymax = 1; //((max - this.ymin) / 6) + max;

    // Note: Yanif commenting out in favor of clamping to 0-1 range.
    // this.ymin = Math.min.apply(null, json['chart_values']['trewscore']);
    // this.ymin = this.ymin - (this.ymin * .03);
    // this.ymin = (this.ymin > json.trewscore_threshold) ? json.trewscore_threshold - 0.1 : this.ymin;
    // this.ymax = Math.max.apply(null, json['chart_values']['trewscore']) * 1.03;
    // this.ymax = (this.ymax < json.trewscore_threshold) ? json.trewscore_threshold + 0.1 : this.ymax;
    this.ymin = 0;
    this.ymax = 1;
    graph(json, severeOnset, shockOnset, this.xmin, this.xmax, this.ymin, this.ymax);
  }
}

function graph(json, severeOnset, shockOnset, xmin, xmax, ymin, ymax) {
  var graphWidth = Math.floor($('#graph-wrapper').width()) - 10;
  $("#graphdiv").width(graphWidth);
  $("#graphdiv").height(graphWidth * .3225);
  $("#graphdiv").css('line-height', Number(graphWidth * .3225).toString() + 'px');
  var placeholder = $("#graphdiv");

  if (json['patient_age'] < 18 ) {
    $('h1 #header-trewscore').text("");
    placeholder.html("");
    placeholder.css('line-height', Number(graphWidth * .3225).toString() + 'px');
    placeholder.append("<span style='text-align: center; vertical-align: middle; color: #777;'>" +
                       "<p><b>Unable to display chart data: this patient meets the TREWS exclusion criteria.</b></p></span>");
    return;
  }
  else if ( json['chart_values']['trewscore'].length == 0 || json['chart_values']['timestamp'].length == 0 ) {
    // update trewscore in header
    $('h1 #header-trewscore').text("");
    placeholder.html("");
    placeholder.css('line-height', Number(graphWidth * .3225).toString() + 'px');
    placeholder.append("<span style='text-align: center; vertical-align: middle; color: #777;'>" +
                       "<p><b>No recent TREWScore data available for this patient.</b></p></span>");
    return;
  }
  else {
    placeholder.css('line-height', "");
  }

  var data = [];
  var dataLength = json['chart_values']['timestamp'].length;
  for (var i = 0; i < dataLength; i += 1) {
    data.push([json['chart_values']['timestamp'][i], json['chart_values']['trewscore'][i]]);
  }

  // console.log(data, xmin, xmax);

  var xlast = json['chart_values']['timestamp'][dataLength - 1];
  var ylast = json['chart_values']['trewscore'][dataLength - 1];

  // update trewscore in header
  $('h1 #header-trewscore').text(Number(ylast).toFixed(2));

  var verticalMarkings = [
    {color: "#555",lineWidth: 1,xaxis: {from: xlast,to: xlast}},
    {color: "#e64535",lineWidth: 1,yaxis: {from: json['trewscore_threshold'],to: json['trewscore_threshold']}}
  ]

  var arrivalx = (json['patient_arrival']['timestamp'] != undefined) ? json['patient_arrival']['timestamp'] * 1000 : null;
  var severeOnsetx = (severeOnset != undefined) ? severeOnset * 1000 : null;
  var shockOnsetx = (shockOnset != undefined) ? shockOnset * 1000 : null;

  var severeOnsety = null;
  var shockOnsety = null;

  if (json['patient_arrival']['timestamp'] != undefined) {
    var arrivalMark = {color: "#ccc",lineWidth: 1,xaxis: {from: arrivalx,to: arrivalx}};
    //var arrivaly = json['chart_values']['trewscore'].indexOf(arrivalx);
    var arrivaly = jQuery.inArray(arrivalx, json['chart_values']['trewscore'])
    verticalMarkings.push(arrivalMark);
  }
  if (severeOnset != undefined) {
    var severeMark = {color: "#ccc",lineWidth: 1,xaxis: {from: severeOnsetx,to: severeOnsetx}};
    //severeOnsety = json['chart_values']['trewscore'].indexOf(severeOnsetx);
    severeOnsety = jQuery.inArray(severeOnsetx, json['chart_values']['trewscore'])
    verticalMarkings.push(severeMark);
  }
  if (shockOnset != undefined) {
    var shockMark = {color: "#ccc",lineWidth: 1,xaxis: {from: shockOnsetx,to: shockOnsetx}};
    //shockOnsety = json['chart_values']['trewscore'].indexOf(shockOnsetx);
    shockOnsety = jQuery.inArray(shockOnsetx, json['chart_values']['trewscore'])
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
      ticks: [[0, "0"], [json['trewscore_threshold'], json['trewscore_threshold']], [1, "1"]],
      tickColor: "#e64535",
      tickLength: 5,
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

      var accessIndex = dataIndex >= json['chart_values']['tf_1_name'].length ?
                          json['chart_values']['tf_1_name'].length - 1 : dataIndex;

      features += "<div class='row cf'>\
              <h4 class='name'>" + humanReadable(json['chart_values']['tf_1_name'][accessIndex]) + "</h4>\
              <h4 class='value'>" + json['chart_values']['tf_1_value'][accessIndex] + "</h4>\
            </div><div class='row cf'>\
              <h4 class='name'>" + humanReadable(json['chart_values']['tf_2_name'][accessIndex]) + "</h4>\
              <h4 class='value'>" + json['chart_values']['tf_2_value'][accessIndex] + "</h4>\
            </div><div class='row cf'>\
              <h4 class='name'>" + humanReadable(json['chart_values']['tf_3_name'][accessIndex]) + "</h4>\
              <h4 class='value'>" + json['chart_values']['tf_3_value'][accessIndex] + "</h4>\
            </div>";

      $("#tooltip").html(features)
        .css({top: item.pageY+5, left: item.pageX+5})
        .show();
    } else {
      $("#tooltip").hide();
    }
  });

  // Chart Drawing Addistions
  var o = plot.pointOffset({ x: xlast, y: ylast});
  if (json.chart_values.timestamp.length == 0) {
    graphTag(plot, xmax, ylast, "Now", "now");
  } else {
    graphTag(plot, xlast, ylast, "Most Recent", "now");
  }
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
      <h3>High Risk<br/>for Deterioration</h3>\
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



/**
 * Criteria Component
 * @param JSON String
 * @return {String} html for a specific criteria
 */
var criteriaComponent = function(c, constants, key, hidden) {
  this.isOverridden = false;
  this.status = "";

  var displayValue = c['value'];
  var precision = constants['precision'] == undefined ? 5 : constants['precision'];

  if ( displayValue && ( isNumber(displayValue) || !isNaN(Number(displayValue)) ) ) {
    displayValue = Number(displayValue).toPrecision(precision);
  }

  var hiddenClass = "";

  // Local conversions.
  if ( c['name'] == 'sirs_temp' ) {
    displayValue = ((Number(displayValue) - 32) / 1.8).toPrecision(3);
  }

  if (c['is_met'] && c['measurement_time']) {
    this.classComplete = " met";
    var lapsed = timeLapsed(new Date(c['measurement_time']*1000));
    var strTime = strToTime(new Date(c['measurement_time']*1000));
    if (c['name'] == 'respiratory_failure') {
      this.status += "Criteria met <span title='" + strTime + "'>" + lapsed + "</span> with <span class='value'>Mechanical Support: On</span>";
    } else {
      this.status += "Criteria met <span title='" + strTime + "'>" + lapsed + "</span> with a value of <span class='value'>" + displayValue + "</span>";
    }
  } else {
    if (c['override_user'] != null) {
      this.classComplete = " unmet";
      this.isOverridden = true;
      if (c['measurement_time']) {
        var cLapsed = timeLapsed(new Date(c['measurement_time']*1000));
        var cStrTime = strToTime(new Date(c['measurement_time']*1000));
        if (c['name'] == 'respiratory_failure') {
          this.status += "Criteria met <span title='" + cStrTime + "'>" + cLapsed + "</span> with <span class='value'>Mechanical Support: On</span>";
        } else {
          this.status += "Criteria met <span title='" + cStrTime + "'>" + cLapsed + "</span> with a value of <span class='value'>" + displayValue + "</span>";
        }
        this.status += (c['override_time']) ? "<br />" : "";
      }
      if (c['override_time']) {
        var oLapsed = timeLapsed(new Date(c['override_time']*1000));
        var oStrTime = strToTime(new Date(c['override_time']*1000));
        this.status += "Customized by " + c['override_user'] + " <span title='" + oStrTime + "'>" + oLapsed + "</span>";
      }
    } else {
      hiddenClass = (hidden) ? " hidden" : " unhidden";
      this.classComplete = " unmet";
      this.status = "";
    }
  }
  var criteriaString = "";
  for (var i = 0; i < constants.overrideModal.length; i++) {
    var crit = null;
    if (c['override_user'] != null) {
      if (trews.getSpecificCriteria(key, constants.key).override_value[i] != undefined) {
        if (trews.getSpecificCriteria(key, constants.key).override_value[i].range == 'min' ||
            trews.getSpecificCriteria(key, constants.key).override_value[i].range == 'max')
        {
          crit = trews.getSpecificCriteria(key, constants.key).override_value[i].lower ? trews.getSpecificCriteria(key, constants.key).override_value[i].lower : trews.getSpecificCriteria(key, constants.key).override_value[i].upper;
        } else {
          crit = [trews.getSpecificCriteria(key, constants.key).override_value[i].lower, trews.getSpecificCriteria(key, constants.key).override_value[i].upper]
        }
      }
    } else {
      crit = (constants.overrideModal[i].value != null) ? constants.overrideModal[i].value : constants.overrideModal[i].values
    }
    var name = constants.overrideModal[i].name
    var unit = constants.overrideModal[i].units
    if ( c['name'] == 'sirs_temp' ) {
      crit[0] = Number(crit[0]).toPrecision(3);
      crit[1] = Number(crit[1]).toPrecision(3);
    }

    if (constants.key == 'respiratory_failure') {
      if (c['is_met']) {
        criteriaString += name;
      } else {
        var with_support = crit == 0 ? 'Off' : 'On';
        criteriaString += name + ": Mechanical Support: " + with_support;
      }
    }
    else if (constants.overrideModal[i].range == 'true') {
      criteriaString += name + " < " + crit[0] + unit + " or > " + crit[1] + unit;
    }
    else {
      var comp = (constants.overrideModal[i].range == 'min') ? "<" : ">";
      criteriaString += name + " " + comp + " " + crit + unit;
    }
    criteriaString += " or ";
  }
  criteriaString = criteriaString.slice(0, -4);
  this.html = "<div class='status" + this.classComplete + hiddenClass + "'>\
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
var taskComponent = function(json, elem, constants, doseLimit) {
  var header = elem.find(".order-details-header");
  if ( header.length ) {
    var link = header.find('.inspect');
    var initialCls = 'hidden';
    var initialMsg = '(see all)';
    if ( link.length ) {
      initialCls = link.hasClass('unhidden') ? 'unhidden' : 'hidden';
      initialMsg = link.hasClass('unhidden') ? '(minimize)' : '(see all)';
    }
    header.html(constants['display_name'] + '<a class="inspect ' + initialCls + '">' + initialMsg + '</a>');
  } else {
    elem.find('h3').html(constants['display_name']);
  }
  elem.removeClass('in-progress');
  elem.removeClass('complete');
  if ( json['status'] == 'Ordered' ) {
    elem.addClass('in-progress');
  }
  else if ( orderStatusCompleted(json) ) {
    elem.addClass('complete');
  }

  // Add clinically inappropriate reason.
  var naMsg = '';
  var naPrefix = 'Clinically Inappropriate';
  if ( json['status'] != null && json['status'].startsWith(naPrefix) ) {
    naMsg = naPrefix;
    if (json['status'].length > naPrefix.length + 1) {
      naMsg += ': ' + json['status'].substr(naPrefix.length + 1);
    }
  }

  // Add custom antibiotics dropdown.
  if (constants['display_name'] == 'Antibiotics') {
    // For tasks with details, override the details if the task is not appropriate.
    var expander = elem.find('.inspect');
    expander.unbind();
    expander.click(function(e) {
      e.stopPropagation();
      if ( $(this).hasClass('hidden') ) {
        $(this).text('(minimize)').removeClass('hidden').addClass('unhidden');
        if ( naMsg != '' ) {
          controller.refreshOrderDetails('antibiotics-details');
        } else {
          endpoints.getPatientData('getAntibiotics');
        }
      } else {
        var detailsCtn = $(".order-details-content[data-trews='antibiotics-details']");
        detailsCtn.find('.status.unhidden').removeClass('unhidden').addClass('hidden');
        $(this).text('(see all)').removeClass('unhidden').addClass('hidden');
      }
    })
  }
  else {
    // For every other type of task, set the status.
    elem.find('.status h4').text(naMsg);
  }
}

/**
 * Order details component, displaying value/status/timestamps
 * of 3hr/6hr interventions.
 */
var orderDetailsComponent = function(order_statuses) {
  this.html = '';
  if ( order_statuses.length === 0 ) {
    this.html += "<h4>No interventions found.</h4>"
  } else {
    for (var i in order_statuses) {
      this.html += "<h4>" + order_statuses[i] + "</h4>"
    }
  }

  this.r = function() {
    return this.html;
  }
}

var deterioration = new function() {
  this.d = $('#other-deter-dropdown')
  this.ctn = $('.other-deter-dropdown-list')
  this.launcher = $('#other-deter-launcher')
  this.remoteInitialized = false;
  this.dirty = false;
  this.init = function() {
    for (var i in DETERIORATIONS) {
      this.ctn.prepend("<li data-trews='" + DETERIORATIONS[i] + "'><img src='img/check.png'>" + DETERIORATIONS[i] + "</li>")
    }
    $('.other-deter-dropdown-list li').click(function() {
      $(this).toggleClass('selected')
      deterioration.dirty = true
    })
    $('.other-deter-dropdown-list input').keyup(function() {
      deterioration.dirty = true;
      if ($(this).val().length > 0)
        $('.other-deter-dropdown-list > div').addClass('selected')
      else
        $('.other-deter-dropdown-list > div').removeClass('selected')
    })
    deterioration.launcher.click(function(e) {
      e.stopPropagation()
      deterioration.d.toggle()
    });
    deterioration.d.click(function(e) {
      e.stopPropagation()
    })
  }
  this.render = function(data) {
    if ( !this.remoteInitialized ) {
      this.remoteInitialized = true;
      if ( data == null ) { return; }

      $('.other-deter-dropdown-list li.selected').each(function(i) {
        $(this).removeClass('selected')
      })
      if (data.deterioration) {
        if (data.deterioration.value) {
          for (var i in data.deterioration.value) {
            $("[data-trews='" + data.deterioration.value[i] + "']").addClass('selected')
          }
        }

        if ( data.deterioration.other ) {
          $('.other-deter-dropdown-list input').val(data.deterioration.other)
          $('.other-deter-dropdown-list > div').addClass('selected')
        } else {
          $('.other-deter-dropdown-list input').val('')
          $('.other-deter-dropdown-list > div').removeClass('selected')
        }
      }
    }
  }
  this.sendOff = function() {
    if (this.dirty && this.dirty != "pending") {
      var selected = []
      $('.other-deter-dropdown-list li.selected').each(function(i) {
        selected.push($(this).text())
      })
      var action = {
        "value": selected,
        "other": $('.other-deter-dropdown-list input').val()
      }
      this.remoteInitialized = true;
      endpoints.getPatientData("set_deterioration_feedback", action);
      deterioration.d.fadeOut(300)
      this.dirty = "pending"
    }
  }
}

var dropdown = new function() {
  this.d = $('#dropdown');
  this.ctn = $("<div id='dropdown-content'></div>");

  // Pre-materialized infection dropdown.
  this.susCtn = $("<div id='dropdown-sus-content'></div>");

  this.init = function() {
    this.initSus(); // Build, but do not append, susCtn.
    this.d.append(this.ctn);
  }

  this.reset = function() {
    $('.edit-btn').removeClass('shown');
    if ( this.susCtn == null ) {
      var infectionOther = $("#infection-other");
      infectionOther.html("");
      this.susCtn = $("#dropdown-sus-content").detach();
    }
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

  this.initSus = function() {
    for (var i in INFECTIONS) {
      var s = $('<h5 class="dropdown-link"></h5>').text(INFECTIONS[i]);
      this.susCtn.append(s);
    }
    this.susCtn.append("<div id='infection-other'></div>")
    this.susCtn.find('#infection-other').unbind()
    this.susCtn.find('#infection-other').click(function(e) {
      e.stopPropagation()
    })
    this.susCtn.find('.dropdown-link').click(function() {
      $('#loading').addClass('waiting').spin(); // Add spinner to page
      var action = dropdown.getAction($(this).text());
      endpoints.getPatientData("suspicion_of_infection", action);
    });
  }

  this.sus = function() {
    var infectionOther = $("#infection-other");
    var otherValue = "";
    if ( trews.data && trews.data.severe_sepsis ) {
      otherValue = trews.data.severe_sepsis.suspicion_of_infection.other ? trews.data.severe_sepsis.suspicion_of_infection.value : "";
    }
    infectionOther.append("<input placeholder='Other' value='" + otherValue + "'/><span>Submit</span>")
    $('#infection-other span').unbind()
    $('#infection-other span').click(function() {
      $('#loading').addClass('waiting').spin(); // Add spinner to page
      var action = {"actionName": "sus-edit", "other": $('#infection-other input').val()}
      endpoints.getPatientData("suspicion_of_infection", action);
      dropdown.d.fadeOut(300);
    })
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
    if (i === 'sus-edit') {
      if ( this.susCtn != null ) {
        this.susCtn.appendTo(this.d);
        this.susCtn = null;
        this.sus();
      }
    }
    else {
      this.editFields(i);
    }
  }

  this.draw = function(d, x, y) {
    d.css({
      top: y + 7,
      left: x - (d.width()/2)
    }).fadeIn(30);
  }

  // Initialization.
  $('body').click(function() {
    $('.edit-btn').removeClass('shown');
    $('.place-order-dropdown-btn').removeClass('shown');
    notifications.n.fadeOut(300);
    activity.a.fadeOut(300);
    dropdown.d.fadeOut(300);
    deterioration.sendOff();
    $('.order-dropdown').fadeOut(300);
    $('#order-inappropriate-dropdown').fadeOut(300);
  });

  $('.edit-btn').click(function(e) {
    e.stopPropagation();

    // Hide everything else.
    notifications.n.fadeOut(300);
    activity.a.fadeOut(300);
    deterioration.sendOff();
    $('.order-dropdown').fadeOut(300);
    $('#order-inappropriate-dropdown').fadeOut(300);

    dropdown.reset();
    $(this).addClass('shown');
    dropdown.fill($(this).attr('data-trews'));
    dropdown.draw(dropdown.d,
      $(this).offset().left + ($(this).width()/2),
      $(this).offset().top + $(this).height());
  });

  $('.place-order-dropdown-btn').click(function(e) {
    e.stopPropagation();

    // Hide everything else
    notifications.n.fadeOut(300);
    activity.a.fadeOut(300);
    dropdown.d.fadeOut(300);
    deterioration.sendOff();
    $('.order-dropdown').fadeOut(300);
    $('#order-inappropriate-dropdown').fadeOut(300);

    $(this).addClass('shown');

    var order_d = $('.order-dropdown[data-trews=' + $(this).attr('data-trews') + ']');
    var parent_card = $('.card[data-trews=' + $(this).attr('parent-card') + ']');

    if ( !(order_d == null || parent_card == null) ) {
      dropdown.draw(order_d,
        (parent_card.offset().left + parent_card.width() + 2) - (order_d.width()/2),
        $(this).offset().top + $(this).height());
    } else {
      throw ('Failed to place ' + ($(this).attr('data-trews')).toUpperCase() + ' order.');
    }
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
    var o = trews.getCriteria(this.slot)[this.criteria]['override_value'];

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

      // Slider callback functions set the text values to show the 'acceptable' range.
      if (data['range'] === "min") {
        slideFunction = function(event, ui) {
          $(".slider-numbers[data-trews='" + data['id'] + "']").text(ui.value + " - " + data['maxAbsolute']);
        }
        oValue = (o == null) ? oValue : (o[index]['lower']) ? o[index]['lower'] : oValue
      } else {
        slideFunction = function(event, ui) {
          $(".slider-numbers[data-trews='" + data['id'] + "']").text(data['minAbsolute'] + " - " + ui.value);
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

      // Sliders also initially show the 'acceptable' range.
      if (data['range'] === "min") {
        $(".slider-numbers[data-trews='" + data['id'] + "']").text($(".slider-range[data-trews='" + data['id'] + "']").slider("value") + " - " + data['maxAbsolute']);
      } else {
        $(".slider-numbers[data-trews='" + data['id'] + "']").text(data['minAbsolute'] + " - " + $(".slider-range[data-trews='" + data['id'] + "']").slider("value"));
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
          criteriaOverride["lower"] = (criteriaOverrideData['range'] == 'min') ? $(".slider-range[data-trews='" + criteria + "']").slider("value") : null;
          criteriaOverride["upper"] = (criteriaOverrideData['range'] == 'max') ? $(".slider-range[data-trews='" + criteria + "']").slider("value") : null;
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
          $(".slider-numbers[data-trews='" + criteriaOverrideData['id'] + "']").text(criteriaOverrideData['value'] + " - " + criteriaOverrideData['maxAbsolute']);
        } else {
          $(".slider-numbers[data-trews='" + criteriaOverrideData['id'] + "']").text(criteriaOverrideData['minAbsolute'] + " - " + criteriaOverrideData['value']);
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

/**
 * Notifications.
 * This component maintains the notification badge that appears in the toolbar.
 */
var notifications = new function() {
  this.n = $('#notifications');
  this.nav = $('#header-notifications');
  this.init = function() {
    this.nav.unbind();
    this.nav.click(function(e) {
      e.stopPropagation();

      // Hide everything else.
      activity.a.fadeOut(300);
      dropdown.d.fadeOut(300);
      deterioration.sendOff();
      $('.order-dropdown').fadeOut(300);
      $('#order-inappropriate-dropdown').fadeOut(300);

      notifications.n.toggle();
    });
    this.n.unbind();
    this.n.click(function(e) {
      e.stopPropagation();
    });
  }
  this.getAlertMsg = function(data) {
    var alertMsg = ALERT_CODES[data['alert_code']];

    if ( data['alert_code'] == '301' || data['alert_code'] == '304' ) {
      var n = trews.getIncompleteSevereSepsis3hr();
      if ( n > 0 ) { alertMsg = String(n) + ' ' + alertMsg; } else { return null; }
    }
    else if ( data['alert_code'] == '302' || data['alert_code'] == '305' ) {
      var n = trews.getIncompleteSevereSepsis6hr();
      if ( n > 0 ) { alertMsg = String(n) + ' ' + alertMsg; } else { return null; }
    }
    else if ( data['alert_code'] == '303' || data['alert_code'] == '306' ) {
      var n = trews.getIncompleteSepticShock();
      if ( n > 0 ) { alertMsg = String(n) + ' ' + alertMsg; } else { return null; }
    }
    return alertMsg;
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
    var renderTs = Date.now();

    for (var i = 0; i < data.length; i++) {
      // Skip notifications scheduled in the future, as part of the Dashan event queue.
      var notifTs = new Date(data[i]['timestamp'] * 1000);
      if (notifTs > renderTs) { continue; }

      // Skip messages if there is no content (used to short-circuit empty interventions).
      var notifMsg = this.getAlertMsg(data[i]);
      if ( notifMsg == undefined ) { continue; }

      // Display the notification.
      var notif = $('<div class="notification"></div>');
      notif.append('<h3>' + notifMsg + '</h3>')
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

    // Highlight next step if we have a code 300, and code 205 has not passed.
    var susCtn = $("[data-trews='sus']");
    if ( !susCtn.hasClass('complete') ) {
      var highlightCls = severeSepsisComponent.highlightSuspicionClass();
      if ( highlightCls != null ) {
        susCtn.addClass(highlightCls);
        susCtn.find('.status').show();
        susCtn.find('.status h4').text('Please enter a suspicion of infection');
      } else {
        susCtn.removeClass('highlight-expired highlight-unexpired');
      }
    } else {
      susCtn.removeClass('highlight-expired highlight-unexpired');
    }

    if ( trews.data['deactivated'] ) {
      // For deactivated patients, we hide the counter, but still show a notification list.
      this.nav.find('b').hide();
    } else if (numUnread == 0) {
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

/**
 * Activity Log or AuditList.
 * This component maintains the activity log badge that appears in the toolbar.
 */
var activity = new function() {
  this.a = $('#activity');
  this.nav = $('#header-activity');
  this.init = function() {
    this.nav.unbind();
    this.nav.click(function(e) {
      e.stopPropagation();

      // Hide everything else.
      notifications.n.fadeOut(300);
      dropdown.d.fadeOut(300);
      deterioration.sendOff();
      $('.order-dropdown').fadeOut(300);
      $('#order-inappropriate-dropdown').fadeOut(300);

      activity.a.toggle();
    });
    this.a.unbind();
    this.a.click(function(e) {
      e.stopPropagation();
    });
  }
  this.getLogMsg = function(data) {
    var order_overrides = [ "antibiotics_order",
                            "blood_culture_order",
                            "crystalloid_fluid_order",
                            "initial_lactate_order",
                            "repeat_lactate_order",
                            "vasopressors_order" ];

    var text_overrides = [ "suspicion_of_infection" ];

    var msg = ""

    var is_order = jQuery.inArray(data.name, order_overrides) >= 0;
    var is_text = jQuery.inArray(data.name, text_overrides) >= 0

    if (data['event_type'] == 'set_deterioration_feedback') {
      if (data.value.other == "" && data.value.value.length == 0) {
        return data['uid'] + " has cleared <b>other conditions driving deterioration</b>"
      }
      msg += data['uid'] + LOG_STRINGS[data['event_type']]
      if (data.value.value.length > 0) {
        for (var i = 0; i < data.value.value.length; i ++) {
          msg += data.value.value[i] + ", "
        }
        if (data.value.other == "") {
          return msg.substring(0, msg.length - 2)
        }
      }
      if (data.value.other != "") {
        msg += data.value.other
      }
    } else if (data['event_type'] == 'override') {
      if (data['clear']) {
        msg += data['uid'] + LOG_STRINGS[data['event_type']]['clear']
        for (var i = 0; i < criteriaKeyToName[data.name].length - 1; i ++) {
          msg += criteriaKeyToName[data.name][i].name + ", "
        }
        if (criteriaKeyToName[data.name].length > 2) {
          msg += "and " + criteriaKeyToName[data.name][criteriaKeyToName[data.name].length - 1].name
        }
        else {
          if (criteriaKeyToName[data.name].length > 1) {
            msg = msg.substring(0, msg.length - 2) + " and "
          }
          msg += criteriaKeyToName[data.name][criteriaKeyToName[data.name].length - 1].name
        }
      } else {
        var action = is_order ? LOG_STRINGS[data['event_type']]['ordered'][0] : LOG_STRINGS[data['event_type']]['customized'][0];
        msg += data['uid'] + action
        for (var i = 0; i < criteriaKeyToName[data.name].length - 1; i ++) {
          if (is_order) {
            msg += criteriaKeyToName[data.name][i].name
              + LOG_STRINGS[data['event_type']]['ordered'][1]
              + data.override_value[i].text
              + ", "
          } else if (is_text) {
            msg += criteriaKeyToName[data.name][i].name
              + LOG_STRINGS[data['event_type']]['customized'][1]
              + data.override_value[i].text
              + ", "
          } else {
            msg += criteriaKeyToName[data.name][i].name
              + LOG_STRINGS[data['event_type']]['customized'][1]
              + UpperLowerToLogicalOperators(data.override_value[i], criteriaKeyToName[data.name][i].units)
              + ", "
          }
        }
        if (criteriaKeyToName[data.name].length > 2) {
          if (is_order) {
            msg += "and " + criteriaKeyToName[data.name][criteriaKeyToName[data.name].length - 1].name
              + LOG_STRINGS[data['event_type']]['ordered'][1]
              + data.override_value[i].text
          } else if (is_text) {
            msg += "and " + criteriaKeyToName[data.name][criteriaKeyToName[data.name].length - 1].name
              + LOG_STRINGS[data['event_type']]['customized'][1]
              + data.override_value[i].text
          } else {
            msg += "and " + criteriaKeyToName[data.name][criteriaKeyToName[data.name].length - 1].name
              + LOG_STRINGS[data['event_type']]['customized'][1]
              + UpperLowerToLogicalOperators(data.override_value[i], criteriaKeyToName[data.name][i].units)
          }
        }
        else {
          if (criteriaKeyToName[data.name].length > 1) {
            msg = msg.substring(0, msg.length - 2) + " and "
          }
          if (is_order) {
            msg += criteriaKeyToName[data.name][criteriaKeyToName[data.name].length - 1].name
              + LOG_STRINGS[data['event_type']]['ordered'][1]
              + data.override_value[i].text
          } else if (is_text) {
            msg += criteriaKeyToName[data.name][criteriaKeyToName[data.name].length - 1].name
              + LOG_STRINGS[data['event_type']]['customized'][1]
              + data.override_value[i].text
          } else {
            msg += criteriaKeyToName[data.name][criteriaKeyToName[data.name].length - 1].name
              + LOG_STRINGS[data['event_type']]['customized'][1]
              + UpperLowerToLogicalOperators(data.override_value[i], criteriaKeyToName[data.name][i].units)
          }
        }
      }
    } else if (data['event_type'] == 'deactivate') {
      event_type = data['deactivated'] ? 'deactivate' : 'activate';
      msg += data['uid'] + LOG_STRINGS[event_type]
    } else {
      msg += data['uid'] + LOG_STRINGS[data['event_type']]
    }
    return msg;
  }
  this.render = function(data) {
    this.a.html('');
    if (data == undefined) {
      this.a.append('<p class="none">Can\'t retrieve actviity log at this time.  <br />Activity Log may be under construction.</p>')
      return;
    }
    if (data.length == 0) {
      this.a.append('<p class="none">No Activity</p>')
      return;
    }

    for (var i = 0; i < data.length; i++) {

      var time = new Date(data[i]['timestamp'] * 1000);

      // Skip messages if there is no content (used to short-circuit empty interventions).
      var msg = this.getLogMsg(data[i]);
      if ( msg == undefined ) { continue; }

      // Display the notification.
      var log = $('<div class="log-item"></div>');
      log.append('<h3>' + msg + '</h3>')
      var subtext = $('<div class="subtext cf"></div>');
      subtext.append('<p>' + timeLapsed(new Date(data[i]['timestamp']*1000)) + '</p>');
      log.append(subtext);
      this.a.prepend(log);
    }
  }
}

/**
 * Toolbar.
 * This component manages all buttons on the toolbar other than the notifications badge.
 */

var toolbar = new function() {
  this.resetNav = $('#header-reset-patient');
  this.activateNav = $('#header-activate-button');
  this.feedback = $('#feedback');
  this.feedbackSuccessHideDelay = 3500;

  this.init = function() {
    // 'Reset patient' button initialization.
    this.resetNav.unbind();
    this.resetNav.click(function(e) {
      $('#loading').addClass('waiting').spin(); // Add spinner to page
      toolbar.resetNav.addClass('loading'); // Toggle button as loading
      var action = trews.data['event_id'] == undefined ? null : { "value": trews.data['event_id'] };
      endpoints.getPatientData('reset_patient', action, toolbar.resetNav);
    });

    // 'Deactivate'/'Activate' button initialization.
    this.deactivateState = true; // Initially set to deactivate.
    this.activateNav.unbind();
    this.activateNav.click(function(e) {
      $('#loading').addClass('waiting').spin(); // Add spinner to page
      toolbar.activateNav.addClass('loading');
      endpoints.getPatientData('deactivate', {'value': !trews.data['deactivated']}, toolbar.activateNav);
    });

    // Feedback dialog initialization.
    $('#header-feedback').click(function() { // Show feedback form
      toolbar.feedback.find('p').html('');  // Clear the previous status when opening.
      toolbar.feedback.show();
    })
    $('#feedback').click(function() { //hide feedback form
      toolbar.feedback.fadeOut(500);
    })
    $('#feedback-cancel').click(function () { //hide feedback form and clear
      toolbar.feedback.find('b').remove()
      toolbar.feedback.find('textarea').val('')
      toolbar.feedback.fadeOut(500);
    })
    $('#feedback .modal-content').click(function(e) { // prevent feedback form from closing when click inside feedback form
      e.stopPropagation();
    })
    $('#feedback-submit').click(function() { // submit feedback form
      postBody = {
        'q': (getQueryVariable('PATID') === false) ? null : getQueryVariable('PATID'),
        'u': (getQueryVariable('USERID') === false) ? null : cleanUserId(getQueryVariable('USERID')),
        'depid': (getQueryVariable('DEP') === false) ? null : getQueryVariable('DEP'),
        'feedback': toolbar.feedback.find('textarea').val()
      }
      controller.sendFeedback(postBody);
    })
  }
  this.feedbackSuccess = function() {
    this.feedback.find('p').html('<b class="success">Feedback Submitted!</b>')
    toolbar.feedback.find('textarea').val('')
    window.setTimeout(function() { toolbar.feedback.fadeOut(500); }, toolbar.feedbackSuccessHideDelay);
  }
  this.feedbackError = function() {
    this.feedback.find('p').html('<b class="error">There was an error, please try again or email trews-jhu@opsdx.io</b>')
  }
  this.render = function(json) {
    this.resetNav.show()
    if ( trews.data['deactivated'] ) {
      this.activateNav.find('span').text('Activate');
    } else {
      this.activateNav.find('span').text('Deactivate');
    }
    this.activateNav.show()
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
function strToTime(str, timeFirst, timestampOnly) {
  var date = new Date(Number(str));
  var y = date.getFullYear();
  var m = date.getMonth() + 1;
  var d = date.getDate();
  var h = date.getHours() < 10 ? "0" + date.getHours() : date.getHours();
  var min = date.getMinutes() < 10 ? "0" + date.getMinutes() : date.getMinutes();
  if (timestampOnly !== undefined && timestampOnly) {
    return h + ":" + min;
  }
  else if (timeFirst) {
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
 * Converts a javascript object
 * {
 *  upper:x,
 *  lower:y,
 *  range: true/min/max
 * }
 * into a logical operator statements like
 * > x and < y
*/
function UpperLowerToLogicalOperators(data, units) {
  if (data.range == "true") {
    return "> " + data.lower + units + " and < " + data.upper + units
  } else if (data.range == "min") {
    return "> " + data.lower + units
  } else if (data.range == "max") {
    return "< " + data.upper + units
  }
}
/**
 * EPICUSERID is padded with plus signs, this removes them
*/
function cleanUserId(userId) {
  return userId.replace(/^\++/, "");
}
