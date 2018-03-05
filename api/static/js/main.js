/**
 * Shims/polyfills.
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

if (!Date.prototype.toISOString) {
  (function() {

    function pad(number) {
      if (number < 10) {
        return '0' + number;
      }
      return number;
    }

    Date.prototype.toISOString = function() {
      return this.getUTCFullYear() +
        '-' + pad(this.getUTCMonth() + 1) +
        '-' + pad(this.getUTCDate()) +
        'T' + pad(this.getUTCHours()) +
        ':' + pad(this.getUTCMinutes()) +
        ':' + pad(this.getUTCSeconds()) +
        '.' + (this.getUTCMilliseconds() / 1000).toFixed(3).slice(2, 5) +
        'Z';
    };

  }());
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
function getRandomIntInclusive(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1)) + min; //The maximum is inclusive and the minimum is inclusive
}

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

function getHeaderHeight() {
  var hdrHeight = parseInt($('#header').css('height'), 10);
  var stsHeight = parseInt($('#status-header').css('height'), 10);
  var discHeight = parseInt($('#disclaimer-header').css('height'), 10);
  discHeight = discHeight == null ? 0 : discHeight;

  return {
    'status': hdrHeight + 'px',
    'disclaimer': (hdrHeight + stsHeight) + 'px',
    'total': (hdrHeight + stsHeight + discHeight) + 'px'
  };
}

function refreshHeaderHeight(tag) {
  // Configure column positioning.
  var newTop = getHeaderHeight();
  $('#status-header').css('top', newTop['status']);
  $('#disclaimer-header').css('top', newTop['disclaimer']);
  $('#left-column').css('top', newTop['total']);
  $('#right-column').css('top', newTop['total']);
  $('#notifications').css('top', newTop['total']);
  $('#activity').css('top', newTop['total']);
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
  dropdown.init();
  overrideModal.init();
  notifications.init();
  activity.init();
  toolbar.init()
  dataRefresher.init();
  notificationRefresher.init();
  deterioration.init();
  timeline.init();
  treatmentOverrideComponent.init();
  $('#fake-console').text(window.location);
  $('#fake-console').hide();
  $('#show-console').click(function() {
    $('#fake-console').toggle();
  })

  refreshHeaderHeight('onload');
};

window.onunload = function() {
  var url = (window.location.hostname.indexOf("localhost") > -1) ?
              "http://localhost:8000/api" :
              window.location.protocol + "//" + window.location.hostname + "/api";

  var postBody = {
    q: (getQueryVariable('PATID') === false) ? null : getQueryVariable('PATID'),
    u: (getQueryVariable('USERID') === false) ? null : cleanUserId(getQueryVariable('USERID')),
    s: (getQueryVariable('TSESSID') === false) ? null : getQueryVariable('TSESSID'),
    depid: (getQueryVariable('DEP') === false) ? null : getQueryVariable('DEP'),
    csn: (getQueryVariable('CSN') === false) ? null : getQueryVariable('CSN'),
    loc: (getQueryVariable('LOC') === false) ? null : getQueryVariable('LOC'),
    actionType: 'close_session',
    action: {
      'session-close': 'unload',
      'session-id': (getQueryVariable('TSESSID') === false) ? null : getQueryVariable('TSESSID')
    }
  };

  $.ajax({
    type: "POST",
    async: false,
    url: url,
    data: JSON.stringify(postBody),
    dataType: "json"
  });
}

window.onerror = function(error, url, line) {
  controller.sendLog({acc:'error', data:'ERR:'+error+' URL:'+url+' L:'+line}, true);
};


window.onresize = function() {
  refreshHeaderHeight('onresize');

  /*
  // Re-render chart.
  graphComponent.render(trews.data.chart_data,
                        (trews.data.severe_sepsis != null ? trews.data.severe_sepsis.onset_time : null),
                        (trews.data.septic_shock != null ? trews.data.septic_shock.onset_time : null),
                        graphComponent.xmin, graphComponent.xmax);
  */
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

      case 'trews_org':
        return this.data['severe_sepsis']['trews_organ_dysfunction']['criteria'];
      case 'trews_organ_dysfunction':
        return this.data['severe_sepsis']['trews_organ_dysfunction']['criteria'];

      case 'tension':
        return this.data['septic_shock']['hypotension']['criteria'];
      case 'fusion':
        return this.data['septic_shock']['hypoperfusion']['criteria'];

      /*
      case 'combined_sepsis':
        return this.data['severe_sepsis']['combined']['criteria'];
      case 'combined_shock':
        return this.data['septic_shock']['combined']['criteria'];
      */
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
      var obj_with_status = null;
      try {
        obj_with_status = JSON.parse(this.data[order_name]['status']);
        if ( !('status' in obj_with_status) ) {
          obj_with_status = this.data[order_name];
        }
      } catch (e) {
        obj_with_status = this.data[order_name];
      }
      return this.data[order_name]['is_met'] && orderStatusCompleted(obj_with_status);
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
    // Initial 2sec delay on initialization, to desync with main data refresher.
    window.setTimeout(function() { notificationRefresher.poll(notificationRefresher); }, 2000);
  }
  this.poll = function(obj) {
    endpoints.getPatientData('poll_notifications');
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
      s: (getQueryVariable('TSESSID') === false) ? null : getQueryVariable('TSESSID'),
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
    // Ensure a valid Patient ID.
    if (postBody['q'] == null) {
      $('#loading p').html("No Patient Identifier entered. Please restart application or contact trews-helpdesk@opsdx.io<br/>" + window.location);
      return;
    }
    $.ajax({
      type: "POST",
      url: this.url,
      data: JSON.stringify(postBody),
      dataType: "json",
      start_time: new Date().getTime(),
    }).done(function(result) {
      $('body').removeClass('waiting');
      if ( toolbarButton ) { toolbarButton.removeClass('loading'); }

      if ( trews.data.chart_data == null && !result.hasOwnProperty('trewsData') ) {
        // Drop any other messages before we get the primary page data.
        appendToConsole('Dropping message ' + JSON.stringify(result));
        return;
      }

      // Check page is already not disabled, or we have the primary page data.
      var disablePage = false;

      if ( trews.data != null && trews.data.profile != null ) {
        disablePage = trews.data.profile['age'] < 18 || trews.data.profile['excluded_units'];
      } else if ( result.hasOwnProperty('trewsData') && result.trewsData.profile != null ) {
        disablePage = result.trewsData.profile['age'] < 18 || result.trewsData.profile['excluded_units'];
      }

      if ( disablePage ) {
        $('#loading').removeClass('waiting').spin(false); // Remove any spinner from the page
        var exclusion_reason = "<b>Patient is less than 18 years old, or has been treated in an excluded hospital unit.</b>";
        var msg = "<b>Disabling TREWS, this patient meets our exclusion criteria.</b>" +
                  "<br/>" + exclusion_reason +
                  "<br/>Please contact trews-helpdesk@opsdx.io for more information.";
        $('#loading p').html(msg);
      }
      else {
        if ( result.hasOwnProperty('trewsData') ) {
          $('#loading').removeClass('waiting').spin(false); // Remove any spinner from the page
          trews.setData(result.trewsData);
          if ( trews.data && trews.data.profile['refresh_time'] != null ) { // Update the Epic refresh time.
            var refreshMsg = 'Last refreshed from Epic at ' + strToTime(new Date(trews.data.profile['refresh_time']*1000), true, true) + '.';
            $('h1 #header-refresh-time').text(refreshMsg);
          }
          controller.refresh();
          controller.refreshOrderDetails('antibiotics-details'); // Refresh order details due to clinically inappropriate updates.
          deterioration.dirty = false
        } else if ( result.hasOwnProperty('notifications') ) {
          trews.setNotifications(result.notifications);
          controller.refreshNotifications();
        } else if ( result.hasOwnProperty('antibiotics_result') ) {
          trews.setAntibiotics(result.antibiotics_result);
          controller.refreshOrderDetails('antibiotics-details');
        }

        // Clear loading screen after everything is rendered.
        $('#loading').html('');
        $('#loading').addClass('done');
      }

      timer.log(this.url, this.start_time, new Date().getTime(), 'success');

    }).fail(function(result) {
      $('body').removeClass('waiting');
      $('#loading').removeClass('waiting').spin(false); // Remove any spinner from the page
      if ( toolbarButton ) { toolbarButton.removeClass('loading'); }
      if (result.status == 400) {
        var msg = result.responseJSON['message'];
        if ( result.responseJSON['standalone'] == null || !result.responseJSON['standalone'] ) {
          msg += ".<br/>  Connection Failed<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews-helpdesk@opsdx.io";
        }
        $('#loading p').html(msg);
        $('#test-data').click(function() {
          endpoints.test();
        });
        return;
      }
      endpoints.numTries += 1;
      if (endpoints.numTries > 3) {
        $('#loading p').html("Connection Failed<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews-helpdesk@opsdx.io");
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
    workflowsComponent.render(
      globalJson["antibiotics_order"],
      globalJson["blood_culture_order"],
      globalJson["crystalloid_fluid_order"],
      globalJson["initial_lactate_order"],
      globalJson["repeat_lactate_order"],
      globalJson["vasopressors_order"],
      globalJson["ui"],
      globalJson['severe_sepsis']['onset_time'],
      globalJson['septic_shock']['onset_time']);
    // graphComponent.refresh(globalJson["chart_data"]);
    notifications.render(globalJson['notifications']);
    activity.render(globalJson['auditlist']);
    toolbar.render(globalJson["severe_sepsis"]);
    deterioration.render(globalJson['deterioration_feedback']);

    // These components have dependencies on workflowsComponent HTML elements
    // (e.g., completed/expired status).
    severeSepsisComponent.render(globalJson["severe_sepsis"]);
    septicShockComponent.render(globalJson["septic_shock"], globalJson['severe_sepsis']['is_met']);
    timeline.render(globalJson);
    treatmentOverrideComponent.render(globalJson["severe_sepsis"], globalJson["septic_shock"], globalJson['ui']);
    careSummaryComponent.render();
    nursingWorkflowComponent.render();

    // Adjust column components as necessary.
    var hdrHeight = getHeaderHeight()['total'];
    var lcolTop = parseInt($('#left-column').css('top'), 10);
    if ( hdrHeight != lcolTop ) {
      refreshHeaderHeight('onrefresh');
    }
  }

  this.refreshNotifications = function() {
    var globalJson = trews.data;
    notifications.render(globalJson['notifications']);
  }

  /*
  this.refreshWorkflowsAndComponents = function() {
    var globalJson = trews.data;
    workflowsComponent.render(
      globalJson["antibiotics_order"],
      globalJson["blood_culture_order"],
      globalJson["crystalloid_fluid_order"],
      globalJson["initial_lactate_order"],
      globalJson["repeat_lactate_order"],
      globalJson["vasopressors_order"],
      globalJson["ui"],
      globalJson['severe_sepsis']['onset_time'],
      globalJson['septic_shock']['onset_time']);

    // These components have dependencies on workflowsComponent HTML elements
    // (e.g., completed/expired status).
    severeSepsisComponent.render(globalJson["severe_sepsis"]);
    septicShockComponent.render(globalJson["septic_shock"], globalJson['severe_sepsis']['is_met']);
  }
  */

  // TODO: handle details for every order type.
  this.refreshOrderDetails = function(order_details_type) {
    var orderType = order_details_type == 'antibiotics-details' ? 'antibiotics_order' : null;
    if ( order_details_type == null || orderType == null ) {
      throw ('Failed to refresh order details');
    }

    var unique_order_elems = [];

    // Add clincially inappropriate as a status.
    // By performing string matching, we are fine if the status field is a json-as-text value.
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
    $('#loading p').html("Javascript Error<span id='test-data'>.</span> Please rest<span id='see-blank'>a</span>rt application or contact trews-helpdesk@opsdx.io");
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
var slotComponent = function(elem, link, display_name, skip_summary, border_with_status, criteria_source, skip_threshold_and_value, skip_complete) {
  this.criteria = {};
  this.elem = elem;
  this.link = link;

  this.display_name             = display_name;
  this.skip_summary             = skip_summary;
  this.border_with_status       = border_with_status;
  this.criteria_source          = criteria_source;
  this.skip_threshold_and_value = skip_threshold_and_value;
  this.skip_complete            = skip_complete;

  this.hasOverridenCriteria = function() {
    var list = []
    for (var c in this.criteria) {
      if (this.criteria[c]['override_user'] != null) {
        list.push(c);
      }
    }
    return list;
  }

  this.r = function(json, constants, criteria_mapping) {
    this.criteria = json['criteria'];
    this.constants = constants;

    // Show slot title.
    if ( this.display_name ) {
      this.elem.find('h3').text(this.constants['display_name']);
      this.elem.find('h3').show();
    } else {
      this.elem.find('h3').text('');
      this.elem.find('h3').hide();
    }

    // Add completion border.
    if ( this.skip_complete ) {
      if (json['is_met']) {
        this.elem.addClass('skip-complete');
      } else {
        this.elem.removeClass('skip-complete');
      }
    } else if ( this.border_with_status ) {
      if (json['is_met']) {
        this.elem.removeClass('incomplete-with-status').addClass('complete-with-status');
      } else {
        this.elem.removeClass('complete-with-status').addClass('incomplete-with-status');
      }
    } else  {
      if (json['is_met']) {
        this.elem.addClass('complete');
      } else {
        this.elem.removeClass('complete');
      }
    }
    if ( this.criteria != null ) {
      this.elem.find('.criteria').html('');
      this.elem.find('.criteria-overridden').html('');

      for (var c in this.criteria) {
        var component = new criteriaComponent(this.criteria[c], this.constants['criteria'][c], this.constants.key, this.link.hasClass('hidden'),
                                              criteria_mapping == null ? null : criteria_mapping[c],
                                              this.criteria_source, this.skip_threshold_and_value);
        if (component.isOverridden) {
          this.elem.find('.criteria-overridden').append(component.r());
        } else {
          this.elem.find('.criteria').append(component.r());
        }

        if ( component.criteria_button != null ) {
          var criteria_button = this.elem.find('.status[data-trews="criteria_' + component.name + '"] .criteria-btn');
          criteria_button.unbind();
          criteria_button.click(function(e) {
            $('#loading').addClass('waiting').spin(); // Add spinner to page
            var action = [];
            var as_enable = $(this).attr('data-as-enable') == 'true';
            var src = $(this).attr('data-criteria-src');
            var dst = $(this).attr('data-criteria-dst');

            // Send one or two overrides based on criteria mapping (e.g., GCS has no CMS mapping).
            if ( src != null ) {
              var a = { "actionName": src };
              if ( as_enable ) {
                a['clear'] = true;
              } else {
                a['value'] = [{'text': 'No Infection'}];
              }
              action.push(a);
            }

            if ( dst != null ) {
              var a = { "actionName": dst };
              if ( as_enable ) {
                a['clear'] = true;
              } else {
                a['value'] = [{'text': 'No Infection'}];
              }
              action.push(a);
            }

            endpoints.getPatientData("override_many", action);
          });
        }
      }

      if ( this.skip_summary != null && !this.skip_summary ) {
        this.elem.find('.num-text').text(json['num_met'] + " criteria met. ");

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
          } else {
            e.data.elem.find('.status.unhidden').removeClass('unhidden').addClass('hidden');
            $(this).text('see all').addClass('hidden');
            this.criteriaHidden = true;
          }
        });
      }
      else {
        this.elem.find('.num').hide()
      }
    }
  }
}


/*
 * Text description as previously used in the header.
 */
function longPatientSummary(with_alert, action_type, with_treatment, with_reset, with_no_risk, with_separate_cms, with_html, more_detail_html) {

  var care_status = null;
  var care_status_priority = null;
  var auto_reset_date = null;

  var cms_status = null;

  if ( !(trews.data == null || trews.data['severe_sepsis'] == null) ) {

    var trews_alerting = 'trews_subalert' in trews.data['severe_sepsis'] ? trews.data['severe_sepsis']['trews_subalert']['is_met'] : false;
    var cms_alerting = trews.data['severe_sepsis']['sirs']['is_met'] && trews.data['severe_sepsis']['organ_dysfunction']['is_met'];

    var alert_disabled = trews.data['ui']['ui_deactivate']['is_met'];

    var not_infected = trews.data['severe_sepsis']['suspicion_of_infection']['update_time'] != null
                        && trews.data['severe_sepsis']['suspicion_of_infection']['value'] == 'No Infection';

    var has_infection = trews.data['severe_sepsis']['suspicion_of_infection']['update_time'] != null
                          && trews.data['severe_sepsis']['suspicion_of_infection']['value'] != null
                          && trews.data['severe_sepsis']['suspicion_of_infection']['value'] != 'No Infection';

    var trews_org_ovr = trews.data['severe_sepsis']['trews_organ_dysfunction']['num_met'] == 0
                          && trews.data['severe_sepsis']['trews_organ_dysfunction']['num_overridden'] > 0;

    var cms_org_ovr = trews.data['severe_sepsis']['organ_dysfunction']['num_met'] == 0
                        && trews.data['severe_sepsis']['organ_dysfunction']['num_overridden'] > 0;

    var not_acute_orgdf = trews_org_ovr || (trews.data['severe_sepsis']['sirs']['is_met'] && cms_org_ovr);

    var sepsis_as_trews = trews.data['severe_sepsis']['is_met']
                            && trews.data['severe_sepsis']['is_trews']
                            && trews.data['severe_sepsis']['onset_time'] == trews.data['severe_sepsis']['trews_onset_time'];

    var sepsis_onset = trews.data['severe_sepsis']['onset_time'];
    var shock_onset = trews.data['septic_shock']['onset_time'];

    var trews_subalert = trews_alerting && (sepsis_onset == null && shock_onset == null);
    var sirs_and_orgdf = cms_alerting && (sepsis_onset == null && shock_onset == null);

    var repeat_status_not_completed = trews.data['repeat_lactate_order']['status'] == null
                                        || !orderStatusCompleted(trews.data['repeat_lactate_order']);

    var repeat_lactate_unneeded = trews.data['repeat_lactate_order']['is_met'] && repeat_status_not_completed;

    var no_hypotension = trews.data['septic_shock']['hypotension']['is_met'] != null
                          && !trews.data['septic_shock']['hypotension']['is_met'];

    var vasopressors_unneeded = trews.data['vasopressors_order']['is_met'] && no_hypotension;

    var num_sev3_complete = 0;
    num_sev3_complete += trews.orderIsDone('initial_lactate_order')   ? 1 : 0;
    num_sev3_complete += trews.orderIsDone('blood_culture_order')     ? 1 : 0;
    num_sev3_complete += trews.orderIsDone('antibiotics_order')       ? 1 : 0;
    num_sev3_complete += trews.orderIsDone('crystalloid_fluid_order') ? 1 : 0;

    var num_sev6_complete = num_sev3_complete;
    // TODO: guard repeat_lactate_unneeded and vasopressors_order below
    // when we have an initial_lactate value in the repeat_lactate_order.
    num_sev6_complete += (trews.data['repeat_lactate_order']['is_met'] || orderStatusCompleted(trews.data['repeat_lactate_order'])) ? 1 : 0;

    var num_sep6_complete = num_sev6_complete;
    num_sep6_complete += (vasopressors_unneeded || trews.orderIsDone('vasopressors_order')) ? 1 : 0;

    var sev3 = $("[data-trews='sev3'] .card-subtitle").html();
    var sev6 = $("[data-trews='sev6'] .card-subtitle").html();
    var sep6 = $("[data-trews='sep6'] .card-subtitle").html();

    var sev3_completed = sev3.indexOf('completed') >= 0;
    var sev6_completed = sev6.indexOf('completed') >= 0;
    var sep6_completed = sep6.indexOf('completed') >= 0;

    var sev3_expired = sev3.indexOf('expired') >= 0;
    var sev6_expired = sev6.indexOf('expired') >= 0;
    var sep6_expired = sep6.indexOf('expired') >= 0;

    var care_completed = sev6_completed || sep6_completed;
    var care_expired = sev3_expired || sev6_expired || sep6_expired;

    var expired_offset = 72 * 60 * 60 * 1000;
    var expired_date = (sepsis_onset * 1000) + expired_offset;

    if ( not_infected ) {
      var not_infected_time = null
      if ( trews_alerting ) {
        not_infected_time = Math.max(trews.data['severe_sepsis']['suspicion_of_infection']['update_time'],
                                     trews.data['severe_sepsis']['trews_subalert']['onset_time']);
      } else {
        not_infected_time = Math.max(trews.data['severe_sepsis']['suspicion_of_infection']['update_time'],
                                     trews.data['severe_sepsis']['sirs']['onset_time'],
                                     trews.data['severe_sepsis']['organ_dysfunction']['onset_time']);
      }

      not_infected_date = new Date(not_infected_time.toFixed(0) * 1000);
      auto_reset_date = new Date(not_infected_date.getTime() + (72 * 60 * 60 * 1000));

      if ( with_alert ) {
        care_status = 'No infection suspected for this patient at ' + strToTime(not_infected_date, true, false) + '. ';
      } else {
        care_status = 'Patient evaluted as not septic. ';
      }
      if ( action_type == 1 ) {
        care_status += '<br>No further sepsis evaluation steps required at this time. '
      }
      care_status_priority = 'no-priority';
    }
    else if ( not_acute_orgdf ) {
      var not_acute_orgdf_time = null;
      var orgdf_criteria =
        trews_org_ovr ?
          trews.data['severe_sepsis']['trews_organ_dysfunction']['criteria']
          : trews.data['severe_sepsis']['organ_dysfunction']['criteria'];

      for (var i in  orgdf_criteria) {
        if ( orgdf_criteria[i]['override_time'] ) {
          not_acute_orgdf_time = not_acute_orgdf_time == null ? orgdf_criteria[i]['override_time']
                                  : Math.min(orgdf_criteria[i]['override_time'], not_acute_orgdf_time);
        }
      }

      not_acute_orgdf_date = new Date(not_acute_orgdf_time.toFixed(0) * 1000);
      auto_reset_date = new Date(not_acute_orgdf_date.getTime() + (72 * 60 * 60 * 1000)); // TODO: check whether this is implemented w/ Andong.

      if ( with_alert ) {
        care_status = 'No acute organ dysfunction due to infection for this patient'
                        + (not_acute_orgdf_date ? ' at ' + strToTime(not_acute_orgdf_date, true, false) : '')+ '. ';

      } else {
        care_status = 'Patient evaluated as not septic. '
      }

      if ( action_type == 1 ) {
        if ( has_infection ) {
          care_status += 'Evaluation steps 1 and 2 complete. '
        }
        if ( more_detail_html ) {
          care_status += more_detail_html;
        }
        if ( has_infection ) {
          care_status += '<br>Please continue to monitor for any organ dysfunction due to infection.'
        } else {
          care_status += '<br>Please complete whether infection is suspected for this patient in evaluation step 1.';
        }
      }
      care_status_priority = 'no-priority';
    }
    else if ( sepsis_onset != null || shock_onset != null ) {

      // Handle scenarios for active, or expired bundles.
      if ( expired_date != null ) {
        auto_reset_date = new Date(expired_date);
      }

      // TODO: subtract 1/0 based on repeat_lactate_unneeded and vasopressors_unneeded when that is implemented.
      var expected_treatments = shock_onset != null ? 6 : 5;
      var actual_treatments = shock_onset != null ? num_sep6_complete : num_sev6_complete;

      if ( shock_onset != null ) {
        if ( trews.data['ui']['ui_septic_shock']['is_met'] ) {
          care_status = 'Patient manually overridden at ' + strToTime(new Date(shock_onset*1000), true, false) + '. ';
        } else if ( with_alert ) {
          care_status = '';
          if ( trews.data['severe_sepsis']['is_trews'] ) {
            care_status += 'TREWS Septic Shock met at ' + strToTime(new Date(shock_onset*1000), true, false) + '. ';
          }

          if ( trews.data['severe_sepsis']['is_cms'] ) {
            if ( !trews.data['severe_sepsis']['is_trews'] ) {
              care_status += 'CMS Septic Shock criteria met at ' + strToTime(new Date(shock_onset*1000), true, false) + '. ';
            }
            else if ( with_separate_cms ) {
              cms_status = 'CMS Septic Shock criteria met at ' + strToTime(new Date(shock_onset*1000), true, false) + '. ';
            }
          }
        }

        if ( action_type == 1 && more_detail_html ) {
          care_status += more_detail_html;
        }
        if ( action_type == 1 && actual_treatments < expected_treatments ) {
          care_status += '<br>Please order missing bundle items under Steps 3 and 5. '
        }
      }
      else if ( sepsis_onset != null ) {
        if ( trews.data['ui']['ui_severe_sepsis']['is_met'] ) {
          care_status = 'Patient manually overridden at ' + strToTime(new Date(sepsis_onset*1000), true, false) + '. ';
        }
        else if ( with_alert ) {
          care_status = '';
          if ( trews.data['severe_sepsis']['is_trews'] ) {
            care_status += 'TREWS Severe Sepsis met at ' + strToTime(new Date(trews.data['severe_sepsis']['trews_onset_time']*1000), true, false)  + '. '
          }

          if ( trews.data['severe_sepsis']['is_cms'] ) {
            if ( !trews.data['severe_sepsis']['is_trews'] ) {
              care_status += 'CMS Severe Sepsis criteria met at ' + strToTime(new Date(trews.data['severe_sepsis']['cms_onset_time']*1000), true, false)  + '. '
            }
            else if ( with_separate_cms ) {
              cms_status = 'CMS Severe Sepsis criteria met at ' + strToTime(new Date(trews.data['severe_sepsis']['cms_onset_time']*1000), true, false)  + '. '
            }
          }
        }

        if ( action_type == 1 && more_detail_html ) {
          care_status += more_detail_html;
        }
        if ( action_type == 1 && actual_treatments < expected_treatments ) {
          care_status += '<br>Please order missing bundle items under Step 3. '
        }
      }

      if ( care_status == null ) {
        care_status = 'Patient on ' + (shock_onset != null ? 'septic shock' : 'sepsis') + ' pathway' + (with_treatment ? ', ' : '.');
      }
      if ( with_treatment ) {
        care_status += actual_treatments + ' of ' + expected_treatments + ' treatment steps complete.'
      }
      care_status_priority = !care_completed ? 'high-priority' : 'low-priority';
    }
    else if ( !alert_disabled && (trews_subalert || sirs_and_orgdf) ) {
      var trews_subalert_onset = new Date(trews.data['severe_sepsis']['trews_subalert']['onset_time'] * 1000);
      var sirs_and_orgdf_onset = new Date(Math.max(trews.data['severe_sepsis']['sirs']['onset_time'], trews.data['severe_sepsis']['organ_dysfunction']['onset_time']) * 1000);

      var trews_prefix = trews_subalert ? 'TREWS triggered for ' + (with_html ? '<b>' : '') + 'high risk of severe sepsis' + (with_html ? '</b>' : '') + ' at ' + strToTime(trews_subalert_onset, true, false) : '';
      var cms_prefix = sirs_and_orgdf ? (with_html ? '<b>' : '') + 'CMS SIRS and Organ Dysfunction' + (with_html ? '</b>' : '') + ' criteria met at ' + strToTime(sirs_and_orgdf_onset, true, false) : '';

      care_status = '';
      if ( with_alert ) {
        care_status += trews_prefix;
        if ( !trews_subalert && sirs_and_orgdf ) {
          care_status += cms_prefix;
        }
        else if ( with_separate_cms && sirs_and_orgdf ) {
          cms_status = cms_prefix + '.';
        }
        care_status += '.'
      } else {
        care_status += (trews_subalert ? 'TREWS alert' : '') + (sirs_and_orgdf ? (trews_subalert ? ' and ' : '') + 'CMS alert' : '') + ' fired,';
      }
      if ( action_type == 1 ) {
        if ( more_detail_html ) {
          care_status += more_detail_html;
        }
        var last_as_comma = care_status[care_status.length - 1] == ',';
        care_status +=  (last_as_comma ? ' please' : '<br>Please') + ' complete evaluation for severe sepsis in steps 1 and 2 below.';
      }
      else if ( action_type == 2 ) {
        var last_as_comma = care_status[care_status.length - 1] == ',';
        care_status +=  (last_as_comma ? ' please' : ' Please') + ' complete the severe sepsis evaluation.';
      }
      care_status_priority = 'low-priority';
    }
    else if ( alert_disabled ) {
      if ( trews.data['ui']['ui_deactivate']['override_value'] != null
            && trews.data['ui']['ui_deactivate']['override_value'].length > 0 )
      {
        if ( trews.data['ui']['ui_deactivate']['override_value'][0]['type'] == 'uncertain' ) {
          if ( with_alert ) {
            care_status = "Patient's sepsis state indicated as uncertain, and further monitoring requested by " + (trews.data['ui']['ui_deactivate']['override_user'] == null ? 'user' : trews.data['ui']['ui_deactivate']['override_user']);
          } else {
            care_status = 'Notifications paused, please keep monitoring the patient';
          }

          var etl_approx_period = 10*60*1000;
          var rounding_period = 5*60*1000;

          var now = Date.now();
          var until = new Date(trews.data['ui']['ui_deactivate']['override_value'][0]['until']);
          until = new Date(Math.ceil((until.getTime() + etl_approx_period) / rounding_period) * rounding_period);

          var when_str = 'at ';
          if ( until <= now ) {
            until = new Date(Math.ceil(now / rounding_period) * rounding_period);
            when_str = 'by ';
          }
          care_status += '. Notifications will resume ' + when_str + strToTime(until, true, false) + '.';
          care_status_priority = 'low-priority';
        }
      }
    }
    else if ( with_no_risk ) {
      var data_ready = false;
      var trews_subalert_json = 'trews_subalert' in trews.data['severe_sepsis'] ? trews.data['severe_sepsis']['trews_subalert'].value : null;
      if ( trews_subalert_json != null ) {
        trews_subalert_json = JSON.parse(trews_subalert_json);
        if ( 'no_lab' in trews_subalert_json && trews_subalert_json['no_lab'] != null
                && typeof(trews_subalert_json['no_lab']) === 'boolean')
        {
          data_ready = !trews_subalert_json['no_lab'];
        }
      }
      if ( data_ready ) {
        care_status = 'TREWS does not indicate high risk of severe sepsis as of ' + strToTime(Date.now(), true, false);
      } else {
        care_status =
          'TREWS does not yet have sufficient data to calculate if this patient is at risk of severe sepsis as of ' + strToTime(Date.now(), true, false) +
          '.<br>If you believe the patient is currently experiencing sepsis, please click Skip to bundle below to enable patient tracking.';
      }
      care_status_priority = 'no-priority';
    }

    if ( with_reset && auto_reset_date != null ) {
      var now = Date.now()
      var remaining = new Date(auto_reset_date.getTime() - now);

      if ( auto_reset_date <= now ) {
        care_status += ' TREWS will reset in approximately 15 minutes.';
      } else {
        var minutes = (remaining.getUTCMinutes() < 10) ? "0" + remaining.getUTCMinutes() : remaining.getUTCMinutes();
        var hours = remaining.getUTCHours();
        var days = remaining.getUTCDate() - 1;

        if ( days == 0 && hours == 0 && minutes <= 15 ) {
          care_status += ' TREWS will reset in approximately 15 minutes.';
        }
        else if ( days >= 0 && hours >= 0 && minutes >= 0 ) {
          care_status += ' TREWS will reset in ' + days + ' days ' + hours + ' hours ' + minutes + ' minutes.';
        }
      }
    }
  }

  return { 'care_status'          : care_status,
           'care_status_priority' : care_status_priority,
           'auto_reset_date'      : auto_reset_date,
           'cms_status'           : cms_status
         }
}


/*
 * Text description as previously used in the card subtitles.
 */
function patientSevereSepsisSummary() {

  if ( !(trews.data == null || trews.data['severe_sepsis'] == null) ) {

    var subtitle = null;
    var subtitle_expired = false;

    var trews_alerting = 'trews_subalert' in trews.data['severe_sepsis'] ? trews.data['severe_sepsis']['trews_subalert']['is_met'] : false;
    var cms_alerting = trews.data['severe_sepsis']['sirs']['is_met'] && trews.data['severe_sepsis']['organ_dysfunction']['is_met'];

    var not_infected = trews.data['severe_sepsis']['suspicion_of_infection']['update_time'] != null
                        && trews.data['severe_sepsis']['suspicion_of_infection']['value'] == 'No Infection';

    var sepsis_onset = trews.data['severe_sepsis']['onset_time'];

    var trews_subalert = trews_alerting && trews.data['severe_sepsis']['suspicion_of_infection']['value'] == null;
    var sirs_and_orgdf = cms_alerting && trews.data['severe_sepsis']['suspicion_of_infection']['value'] == null;

    if ( not_infected ) {
      subtitle = 'Patient evaluated as not septic.';
    }
    else if ( sepsis_onset != null ) {
      var sev6 = $("[data-trews='sev6'] .card-subtitle").html();
      var sev6_completed = sev6.indexOf('completed') >= 0;

      if ( trews.data['ui']['ui_severe_sepsis']['is_met'] || trews.data['ui']['ui_septic_shock']['is_met'] ) {
        subtitle = 'Patient manually overridden for severe sepsis';
      } else {
        subtitle = 'Patient evaluation indicates severe sepsis';
      }

      if ( sev6_completed ) {
        subtitle += ', and the required interventions are complete. Please monitor the patient.';
      } else  {
        subtitle += '. Please complete the required interventions.';
        subtitle_expired = true;
      }
    }
    else if ( trews_subalert || sirs_and_orgdf ) {
      var trews_prefix = trews_subalert ? 'TREWS triggered for high risk of severe sepsis' : '';
      var cms_prefix = sirs_and_orgdf ? ((trews_prefix.length > 0 ? ' and ' : '') + 'CMS alert criteria met') : '';
      subtitle = trews_prefix + cms_prefix + '. If you suspect infection is the cause, treat patient for severe sepsis.';
      subtitle_expired = true;
    }

    return {'subtitle': subtitle, 'subtitle_expired': subtitle_expired};
  }
  return null;
}


/*
 * Text description as previously used in the card subtitles.
 */
function patientSepticShockSummary() {

  if ( !(trews.data == null || trews.data['septic_shock'] == null) ) {
    var subtitle = null;
    var subtitle_expired = false;
    var shock_onset = trews.data['septic_shock']['onset_time'];
    if ( shock_onset != null ) {
      var sep6 = $("[data-trews='sep6'] .card-subtitle").html();
      var sep6_completed = sep6.indexOf('completed') >= 0;

      if ( trews.data['ui']['ui_septic_shock']['is_met'] ) {
        subtitle = 'Patient manually overridden for septic shock';
      } else {
        subtitle = 'Patient evaluation indicates septic shock';
      }

      if ( sep6_completed ) {
        subtitle += ', and the required interventions are complete. Please monitor the patient.';
      } else  {
        subtitle += '. Please complete the required interventions.';
        subtitle_expired = true;
      }
    }
    else if ( trews.data['septic_shock']['crystalloid_fluid']['is_met']
              && !trews.data['septic_shock']['hypotension']['is_met']
              && !trews.data['septic_shock']['hypoperfusion']['is_met'] )
    {
      subtitle = 'Fluids administered but no persistent hypotension or hypoperfusion. No action required.';
    }
    else if ( !(trews.data['septic_shock']['crystalloid_fluid']['is_met']
                  || trews.data['septic_shock']['hypotension']['is_met']
                  || trews.data['septic_shock']['hypoperfusion']['is_met']) )
    {
      subtitle = 'No septic shock criteria met. No action required.';
    }

    return {'subtitle': subtitle, 'subtitle_expired': subtitle_expired};
  }

  return null;
}


/**
 * Component rendering a patient summary.
 */
var careSummaryComponent = new function() {
  this.ctn = $("[data-trews='care-summary']");

  // Slot arguments:
  // elem, link, display_name, skip_summary, border_with_status, criteria_source, skip_threshold_and_value, skip_complete

  this.detailVisible = false;
  this.detailSlot = new slotComponent($("[data-trews='care-summary-detail']"), $('#expand-care-detail'), false, false, false, null, false, true);

  this.renderDetail = function(alert_as_cms, cms_status) {
    var trews_html = '<h3> TREWS Criteria </h3>';

    if ( 'feature_relevances' in trews.data && trews.data['feature_relevances'] != null
          && 'measurements' in trews.data && trews.data['measurements'] != null
          && 'static_features' in trews.data && trews.data['static_features'] != null )
    {
      if ('respiratory rate' in trews.data['feature_relevances']){
        trews.data['feature_relevances']['resp rate'] = trews.data['feature_relevances']['respiratory rate'];
      }

      //var mark = "<font color='red' size=5><b>!</b></font>";
      var mark = "<font color='red' size=5>&#9733</font>";
      var phys_feats = ["BP", "temperature", "heart rate", "SpO2", "PaO2", "PaCO2", "resp rate", "FiO2", "GCS", "RASS"];
      var hem_feats = ["platelets", "WBC", "INR", "hematocrit", "hemoglobin"];
      var chem_feats = ["sodium", "creatinine", "bilirubin", "amylase", "lactate", "BUN", "ALT liver enzymes", "arterial ph", "bicarbonate", "CO2", "AST liver enzymes", "potassium", "lipase"];
      var displayNames = {"ALT liver enzymes": "ALT", "AST liver enzymes": "AST", 'temperature':'Temperature', "heart rate": "Heart Rate", "resp rate": "Resp. Rate", "platelets": "Platelets","hematocrit":"hematocrit","hemoglobin":"Hemoglobin","sodium": "Sodium", "creatinine":"Creatinine", "bilirubin":"Bilirubin","amylase":"Amylase", "lactate":"Lactate","arterial ph":"Arterial PH","bicarbonate":"Bicarbonate","potassium":"Potassium","lipase":"Lipase"};
      var no_features_str = "";
      if (Object.keys(trews.data['feature_relevances']).length == 0) {
        no_features_str = '<div style="background-color:yellow"><h3 style="color:black">TREWS alerted based on many factors without a dominant feature</h3></div>';
      }

      var phys_table_str = '<table style="width:100%;background-color:white;">'
      for (var i = 0; i < phys_feats.length; i++) {
          phys_table_str += '<tr>'
          var feat = phys_feats[i];
          phys_table_str += '<td>' +(feat in trews.data['feature_relevances']? mark:"&nbsp")+'</td>';
          phys_table_str += '<td>'+(feat in displayNames ? displayNames[feat]:feat)+'</td>';

          var value = 'Not available'
          feat = feat.toLowerCase().replace(/ /g, "_");
          if (feat in trews.data['measurements']) {
            value = trews.data['measurements'][feat]['value']+' @ '
            var date = new Date(Date.parse(trews.data['measurements'][feat]['tsp'] + " UTC"));
            value += strToTime(date.getTime(),true,false);
          }

          phys_table_str += '<td>'+value+'</td>';
          phys_table_str += '</tr>';
      }

      var hem_table_str = '<table style="width:100%">'
      for (var i = 0; i < hem_feats.length; i++) {
          hem_table_str += '<tr>'
          var feat = hem_feats[i];
          hem_table_str += '<td>' +(feat in trews.data['feature_relevances']? mark:"&nbsp")+'</td>';
          hem_table_str += '<td>'+(feat in displayNames ? displayNames[feat]:feat)+'</td>';
          var value = "Not available"
          feat = feat.toLowerCase().replace(/ /g, "_");
          if (feat in trews.data['measurements']) {
            value = trews.data['measurements'][feat]['value']+' @ '
            var date = new Date(Date.parse(trews.data['measurements'][feat]['tsp'] + " UTC"));
            value += strToTime(date.getTime(),true,false);
          }
          hem_table_str += '<td>'+value+'</td>';
          hem_table_str += '</tr>';
      }

      var chem_table_str = '<table style="width:100%">'
      for (var i = 0; i < chem_feats.length; i++) {
          chem_table_str += '<tr>'
          var feat = chem_feats[i];
          chem_table_str += '<td>' +(feat in trews.data['feature_relevances']? mark:"&nbsp")+'</td>';
          chem_table_str += '<td>'+(feat in displayNames ? displayNames[feat]:feat)+'</td>';

          var value = "Not available"
          feat = feat.toLowerCase().replace(/ /g, "_");
          if (feat in trews.data['measurements']) {
            value = trews.data['measurements'][feat]['value']+' @ '
            var date = new Date(Date.parse(trews.data['measurements'][feat]['tsp'] + " UTC"));
            value += strToTime(date.getTime(),true,false);
         }
          chem_table_str += '<td>'+value+'</td>';
          chem_table_str += '</tr>';
      }

      var static_table_str = '<table style="width:100%">';
      for (var feat in trews.data['static_features']) {
        static_table_str += '<tr>'
        static_table_str += '<td>' +(feat in trews.data['feature_relevances']? mark:"&nbsp")+'</td>';
        static_table_str += '<td>'+ (feat.charAt(0).toUpperCase()+feat.slice(1)).replace(/_/g," ")+'</td>';
        static_table_str += '<td>' + (trews.data['static_features'][feat]==1 ? "Present":trews.data['static_features'][feat]) + '</td>';
        static_table_str += '</tr>'
      }

      trews_html = '<h3> TREWS Criteria </h3><table style="width:100%">'
                     + no_features_str
                     + '<tr><th>Physiology</th><th>Hematology and coagulation</th><th>Chemistry</th><th>Demographics and History</th></tr>'
                     + '<tr>'
                     + '<td style="vertical-align:top">' + phys_table_str + '</table></td>'
                     + '<td style="vertical-align:top">' + hem_table_str + '</table></td>'
                     + '<td style="vertical-align:top">' + chem_table_str + '</table></td>'
                     + '<td style="vertical-align:top">' + static_table_str + '</table></td>'
                     + '</tr></table>';
    }
    this.detailSlot.elem.find('.trews-criteria').html(trews_html);
  }

  this.render = function() {
    var trews_alerting = 'trews_subalert' in trews.data['severe_sepsis'] ? trews.data['severe_sepsis']['trews_subalert']['is_met'] : false;
    var cms_alerting = trews.data['severe_sepsis']['sirs']['is_met'] && trews.data['severe_sepsis']['organ_dysfunction']['is_met'];

    var alert_disabled = trews.data['ui']['ui_deactivate']['is_met'];

    var only_cms_alerting = !trews_alerting && cms_alerting;
    var only_cms_is_sepsis = !trews.data['severe_sepsis']['is_trews'] && trews.data['severe_sepsis']['is_cms'];

    var has_any_alert = !alert_disabled && (trews_alerting || cms_alerting);
    var alert_as_cms = only_cms_alerting || only_cms_is_sepsis;

    // longPatientSummary args: with_alert, action_type, with_treatment, with_reset, with_no_risk, with_separate_cms, more_detail_html
    var more_detail_html = '&nbsp;&nbsp;<span class="summary-more-detail">More Detail</span>';
    var patient_summary = longPatientSummary(true, 1, false, false, true, true, true, has_any_alert ? more_detail_html : null);
    var summary_html = patient_summary.care_status;
    this.ctn.find('h4').html(summary_html);

    if ( has_any_alert ) {
      this.renderDetail(alert_as_cms, patient_summary.cms_status);
    }

    if ( this.detailVisible ) {
      this.detailSlot.elem.removeClass('hidden').addClass('unhidden');
      if ( has_any_alert ) {
        this.ctn.find('h4 span.summary-more-detail').text('Less Detail');
      }
    } else {
      this.detailSlot.elem.removeClass('unhidden').addClass('hidden');
      if ( has_any_alert ) {
        this.ctn.find('h4 span.summary-more-detail').text('More Detail');
      }
    }

    if ( has_any_alert ) {
      this.ctn.find('h4 span.summary-more-detail').unbind();
      this.ctn.find('h4 span.summary-more-detail').click(function(e) {
        if ( careSummaryComponent.detailSlot.elem.hasClass('hidden') ) {
          careSummaryComponent.detailSlot.elem.removeClass('hidden').addClass('unhidden');
        } else {
          careSummaryComponent.detailSlot.elem.removeClass('unhidden').addClass('hidden');
        }
        careSummaryComponent.detailVisible = !careSummaryComponent.detailVisible;
        careSummaryComponent.ctn.find('h4 span.summary-more-detail').text(careSummaryComponent.detailVisible ? 'Less Detail' : 'More Detail');
      });
    }
  }
}

/**
 *  Nursing workflow component
 *  Allows nurses to evaluate the patient
 */

var nursingWorkflowComponent = new function() {
	this.ctn = $("[data-trews='nurse-workflow']");
        this.status_buttons = {"Yes": '#yes_mental_stat', "No":'#no_mental_stat', "Unknown":'#unk_mental_stat'};
        this.inf_buttons = {"Yes":'#yes_inf', "No":'#no_inf'};
	//this.notif_buttons = {"Yes":'#yes_notif', "No":'#no_notif'};
	this.render = function() {
		//hide the display if no alert
		if (!trews.data["severe_sepsis"]["trews_subalert"]["is_met"]) {
			console.log("Seting display to none");
			this.ctn.html("");
			return;
		}
		//timestamp of the last evaluation
		var time_txt = "";
		if ("nursing_eval" in trews.data && "tsp" in trews.data["nursing_eval"]) {
            		//var date = new Date(Date.parse(trews.data['nursing_eval']['tsp'] + " UTC"));
			var date = new Date(1000*trews.data['nursing_eval']['tsp']);
			time_txt = "The last evaluation was at " + strToTime(date.getTime(),true,false);
			if (Date.now()-date.getTime() > (12 * 60*60*1000)) {
				time_txt += ", over 12 hours ago. Please confirm and resubmit the evaluation";
			}
			time_txt += " by " + trews.data["nursing_eval"]["uid"];
		} else {
			date = null;
			time_txt = "An evaluation has not been completed";
		}
		this.ctn.find('#time_stat').text(time_txt);
		
		
               //Set states
		console.log("setting states");
		$('#save_comment').checked="true";
		console.log("set save_comment");
		console.log($('#save_comment').checked);
		this.eval_box = $('#eval_comments')
		if ("nursing_eval" in trews.data && "comments" in trews.data["nursing_eval"]) {
			this.eval_box[0].value = trews.data["nursing_eval"]["comments"];
		}
                if ("nursing_eval" in trews.data) {
                  if ("mental_status" in trews.data["nursing_eval"] && trews.data["nursing_eval"]["mental_status"] in this.status_buttons) {
			console.log("setting status button");
			$(this.status_buttons[trews.data["nursing_eval"]["mental_status"]])[0].checked="true";
			
		   }
		  if ("known_infection" in trews.data["nursing_eval"] && trews.data["nursing_eval"]["known_infection"] in this.inf_buttons) {
			console.log("setting inf button");
			$(this.inf_buttons[trews.data["nursing_eval"]["known_infection"]])[0].checked="true";
		   }
		  if ("provider_notified" in trews.data["nursing_eval"]) {
			$('#yes_notif')[0].checked="true";
		   }
		}
		this.update_notification_prompt();
		
		console.log("done setting states");
		this.yes_mental_btn = $('#yes_mental_stat');
		this.yes_mental_btn.click(function(e){mental_status_click("Yes")});
		this.no_mental_btn = $('#no_mental_stat');
		this.no_mental_btn.click(function(e){mental_status_click("No")});
		this.unk_mental_btn = $('#unk_mental_stat');
		this.unk_mental_btn.click(function(e){mental_status_click("Unknown")});
		this.no_inf_btn = $('#no_inf');
		this.no_inf_btn.click(function(e){infection_click("No")});
		this.yes_inf_btn = $('#yes_inf');
		this.yes_inf_btn.click(function(e){infection_click("Yes")});
		$('#yes_notif').click(function(e){notify_click()});
		this.save_btn = $('#save_comment');
		this.save_btn.click(function(e){save_comment($('#eval_comments')[0].value)});
 	}
	this.update_notification_prompt = function () {
		var no_threshold = 20;
		var notify_txt = "TREWS indicates high risk of sepsis, please notify the patient's provider";
		var no_notify_txt = "TREWS does not indicate high risk of sepsis at this time.";
		var result_txt = "";
		console.log("updating notif");
		if ("mental_status" in trews.data["nursing_eval"] && "known_infection" in trews.data["nursing_eval"]) {
			if ( trews.data["nursing_eval"]["mental_status"] == 'Yes' || trews.data["nursing_eval"]["known_infection"] == 'Yes') {
				trews.data["nursing_eval"]["advise_notify"] = "true";
				console.log("updating to notif_txt from yes");
				result_txt = notify_txt;
			} else if (JSON.parse(trews["data"]["severe_sepsis"]["trews_subalert"]["value"])["pct_sevsep"]>no_threshold) {
				trews.data["nursing_eval"]["advise_notify"] = "true";
				console.log("updating to notif_txt from prob");
				result_txt = notify_txt;
			} else {
				trews.data["nursing_eval"]["advise_notify"] = "false";
				console.log("updating to no_notif");
				result_txt = no_notify_txt;
			}
		} else { 
			trews.data["nursing_eval"]["advise_notify"] = "true";
		}
		this.ctn.find("#notify_stat").text(result_txt);
		console.log("Advise_notify:" + trews.data["nursing_eval"]["advise_notify"])
		if (trews.data["nursing_eval"]["advise_notify"] == "true") {
			console.log("setting to advise")
			$('#provider-notified-block')[0].style="display:inline-block;";
		} else {
			console.log("setting to hidden")
			$('#provider-notified-block')[0].style="display:none;";
		}
	}
}

var mental_status_click = function(stat) {
	if (!("nursing_eval" in trews.data)){
		trews.data["nursing_eval"] = {};
	}
	trews.data["nursing_eval"]["mental_status"] = stat;
	updateNursingEval()
	
}
var infection_click = function(stat) {
	if (!("nursing_eval" in trews.data)){
		trews.data["nursing_eval"] = {};
	}
	trews.data["nursing_eval"]["known_infection"] = stat;
	updateNursingEval()

}
var notify_click = function() {
	if (!("nursing_eval" in trews.data)){
		trews.data["nursing_eval"] = {};
	}
	trews.data["nursing_eval"]["provider_notified"] = "true";
	updateNursingEval()

}
var save_comment = function(comment) {
	if (!("nursing_eval" in trews.data)){
		trews.data["nursing_eval"] = {};
	}
	trews.data["nursing_eval"]["comments"] = comment;
	updateNursingEval()
	
}

var updateNursingEval = function() {
	console.log("updating nurse eval");
	nursingWorkflowComponent.update_notification_prompt();
      	endpoints.getPatientData("update_nursing_eval",trews.data["nursing_eval"]);
}

/**
 * Treatment override component.
 * Allows users to enable the interventions by directly clicking a toggle slider.
 */
var treatmentOverrideComponent = new function() {
  this.uncertain_sevsep_ctn = $('[data-trews="uncertain-sepsis"]');
  this.uncertain_sevsep_btn = null;

  this.sevsep_override_ctn = $('[data-trews="treat-sepsis"]');
  this.sevsep_override_btn = null;

  this.sepshk_override_ctn = $('[data-trews="treat-shock"]');
  this.sepshk_override_btn = null;

  this.init = function() {
    this.uncertain_sevsep_btn = $('#uncertain-sepsis-toggle');
    this.sevsep_override_btn = $('#severe-sepsis-toggle');
    this.sepshk_override_btn = $('#septic-shock-toggle');

    // Add click handlers. Note these will be ignored by the parent's inactive class as present.
    // this.uncertain_sevsep_btn.unbind();
    this.uncertain_sevsep_btn.click(function(e) {
      $('#loading').addClass('waiting').spin(); // Add spinner to page
      var action = { "actionName": 'ui_deactivate', 'is_met': e.target.checked };
      if ( e.target.checked ) {
        var x = new Date(Date.now() + 50*60*1000);
        action['value'] = [{'type': 'uncertain', 'until': x.toISOString()}];
      } else {
        action['clear'] = true;
      }
      endpoints.getPatientData("override", action);
    });

    // this.sevsep_override_btn.unbind();
    this.sevsep_override_btn.click(function(e) {
      $('#loading').addClass('waiting').spin(); // Add spinner to page
      var action = { "actionName": 'ui_severe_sepsis', 'is_met': e.target.checked };
      if ( e.target.checked ) {
        action['value'] = [{'text': 'dummy'}];
      } else {
        action['clear'] = true;
      }
      endpoints.getPatientData("override", action);
    });

    // this.sevsep_override_btn.unbind();
    this.sepshk_override_btn.click(function(e) {
      $('#loading').addClass('waiting').spin(); // Add spinner to page
      var action = { "actionName": 'ui_septic_shock', 'is_met': e.target.checked };
      if ( e.target.checked ) {
        action['value'] = [{'text': 'dummy'}];
      } else {
        action['clear'] = true;
      }
      endpoints.getPatientData("override", action);
    });
  }

  this.render = function(sspJSON, sshJSON, uiJSON) {

    // Reset uncertain sepsis button.
    this.uncertain_sevsep_ctn.find('h4').removeClass('inactive');
    this.uncertain_sevsep_ctn.find('.onoffswitch').removeClass('inactive');

    // Reset severe sepsis manual override.
    this.sevsep_override_ctn.find('h4').removeClass('inactive');
    this.sevsep_override_ctn.find('.onoffswitch').removeClass('inactive');

    // Reset septic shock manual override.
    this.sepshk_override_ctn.find('h4').removeClass('inactive');
    this.sepshk_override_ctn.find('.onoffswitch').removeClass('inactive');

    // Disable uncertain toggle button if an infection is present.
    var any_override = uiJSON['ui_severe_sepsis']['is_met'] || uiJSON['ui_septic_shock']['is_met'];

    var uncertain_inactive = any_override || sspJSON['is_met'] || sshJSON['is_met']
                              || trews.data['severe_sepsis']['suspicion_of_infection']['value'] != null;

    var ui_deactivated = !uncertain_inactive && uiJSON['ui_deactivate']['is_met'];

    if ( uncertain_inactive ) {
      this.uncertain_sevsep_ctn.find('h4').addClass('inactive');
      this.uncertain_sevsep_ctn.find('.onoffswitch').addClass('inactive');
    }
    else {
      this.uncertain_sevsep_btn.attr('checked', uiJSON['ui_deactivate']['is_met']);

      if ( !uiJSON['ui_deactivate']['is_met'] || uiJSON['ui_deactivate']['override_value'] == null ) {
        this.uncertain_sevsep_ctn.find('h4').text('Re-evaluate in 1 hr');
      }
      else if ( uiJSON['ui_deactivate']['override_value'] != null ) {
        var now = Date.now();

        var etl_approx_period = 10*60*1000;
        var rounding_period = 5*60*1000;

        var now = Date.now();
        var until = new Date(uiJSON['ui_deactivate']['override_value'][0]['until']);
        until = new Date(Math.ceil((until.getTime() + etl_approx_period) / rounding_period) * rounding_period);

        if ( until <= now ) {
          until = new Date(now + rounding_period);
        }

        var remaining = new Date(until - now);
        var minutes = remaining.getUTCHours() * 60 + remaining.getUTCMinutes();
        var suffix = minutes == 1 ? ' minute' : ' minutes';

        minutes = Math.max(Math.ceil(minutes / 5) * 5, 5);
        var prefix = minutes <= 15 ? 'approximately ' : '';
        this.uncertain_sevsep_ctn.find('h4').text('Re-evaluate in ' + prefix + minutes + suffix);
      }
    }

    if ( ui_deactivated || uiJSON['ui_septic_shock']['is_met'] || ( !any_override && (sspJSON['is_met'] || sshJSON['is_met']) ) ) {
      this.sevsep_override_ctn.find('h4').addClass('inactive');
      this.sevsep_override_ctn.find('.onoffswitch').addClass('inactive');
    }
    else {
      this.sevsep_override_btn.attr('checked', uiJSON['ui_severe_sepsis']['is_met']);
    }

    if ( ui_deactivated || (!any_override && sshJSON['is_met']) ) {
      this.sepshk_override_ctn.find('h4').addClass('inactive');
      this.sepshk_override_ctn.find('.onoffswitch').addClass('inactive');
    }
    else {
      this.sepshk_override_btn.attr('checked', uiJSON['ui_septic_shock']['is_met']);
    }

    /*
    if ( any_override ) {
      var sep_t = uiJSON['ui_severe_sepsis']['override_time'] == null ? uiJSON['ui_septic_shock']['override_time'] : uiJSON['ui_severe_sepsis']['override_time'];
      var shk_t = uiJSON['ui_septic_shock']['override_time'] == null ? uiJSON['ui_severe_sepsis']['override_time'] : uiJSON['ui_septic_shock']['override_time'];

      var override_t = Math.min(sep_t, shk_t);
      var override_u = uiJSON['ui_severe_sepsis']['override_time'] == override_t ? uiJSON['ui_severe_sepsis']['override_user'] : uiJSON['ui_septic_shock']['override_user'];

      var lapsed = timeLapsed(new Date(override_t*1000));
      var strTime = strToTime(new Date(override_t*1000));
      $('.bundle-override h5').html('Overridden by ' + override_u + " <span title='" + strTime + "'>" + lapsed);
    } else {
      $('.bundle-override h5').html('');
    }
    */
  }
}


/**
 * Severe Sepsis Component.
 * Responsible for rendering severe sepsis evaluation components, including
 * SOI input, acute organ dysfunction, criteria summary, and severe sepsis presence
 */
var severeSepsisComponent = new function() {
  this.sus = {};
  this.ctn = $("[data-trews='severeSepsis']");
  this.susCtn = $("[data-trews='sus']");
  this.acuteOrgCtn = $("[data-trews='eval-acute-orgdf']");
  this.noInfectionBtn = $('.no-infection-btn');

  for (var i in INFECTIONS) {
    var s = $('<option></option>').text(INFECTIONS[i]);
    $('.selection select').append(s);
  }

  // Slot arguments:
  // elem, link, display_name, skip_summary, border_with_status, criteria_source, skip_threshold_and_value, skip_complete

  this.orgSlot = new slotComponent($("[data-trews='eval-orgdf']"), $('#expand-org'), false, true, false, null, false, true);


  // Returns the class to attach to the SOI slot (to highlight it).
  // Returns null if no highlighting is to be performed.
  this.highlightSuspicionClass = function() {
    // Criteria-based highlighting
    var sepsis_onset = trews.data['severe_sepsis']['onset_time'];
    var shock_onset = trews.data['septic_shock']['onset_time'];

    var trews_subalert = ('trews_subalert' in trews.data['severe_sepsis'] ? trews.data['severe_sepsis']['trews_subalert']['is_met'] : false)
                        && (sepsis_onset == null && shock_onset == null);

    var sirs_and_orgdf = trews.data['severe_sepsis']['sirs']['is_met']
                        && trews.data['severe_sepsis']['organ_dysfunction']['is_met']
                        && (sepsis_onset == null && shock_onset == null);

    return ( trews_subalert || sirs_and_orgdf ) ? 'highlight-expired' : null;
  }

  this.suspicion = function(json) {
    this.susCtn.find('h3').text(json['display_name']);
    this.susCtn.removeClass('inactive complete complete-with-status complete-no-infection highlight-expired highlight-unexpired');
    var alert_snoozed = trews.data['ui']['ui_deactivate']['is_met'];

    if ( this.sus['value'] == null && alert_snoozed ) {
      this.susCtn.addClass('inactive');
    }
    else if (this.sus['value'] == null) {
      var highlightCls = this.highlightSuspicionClass();
      if ( highlightCls != null ) {
        this.susCtn.addClass(highlightCls);
        this.susCtn.find('.status').show();
        this.susCtn.find('.status h4').text('');
        //this.susCtn.find('.status h4').text('Please indicate whether infection is suspected');
      } else {
        this.susCtn.find('.status').hide();
      }
      this.susCtn.find('.status h5').html('');
    } else {
      if (this.sus['value'] != 'No Infection') {
        this.susCtn.addClass('complete-with-status');
      }
      else {
        this.susCtn.addClass('complete-no-infection');
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
    this.ctn.find('h2.card-header').text(severe_sepsis['display_name']);

    var trews_alerting = 'trews_subalert' in json ? json['trews_subalert']['is_met'] : false;
    var cms_alerting = json['sirs']['is_met'] && json['organ_dysfunction']['is_met'];

    var trews_org_ovr = trews.data['severe_sepsis']['trews_organ_dysfunction']['num_met'] == 0
                          && trews.data['severe_sepsis']['trews_organ_dysfunction']['num_overridden'] > 0;

    var cms_org_ovr = trews.data['severe_sepsis']['organ_dysfunction']['num_met'] == 0
                        && trews.data['severe_sepsis']['organ_dysfunction']['num_overridden'] > 0;

    var not_acute_orgdf = trews_org_ovr || (trews.data['severe_sepsis']['sirs']['is_met'] && cms_org_ovr);

    this.ctn.removeClass('complete complete-no-infection');
    if (json['is_met']) {
      this.ctn.addClass('complete');
    } else if ( json['suspicion_of_infection']['value'] == 'No Infection' ) {
      this.ctn.addClass('complete-no-infection');
    }

    this.sus = json['suspicion_of_infection'];
    this.suspicion(severe_sepsis['suspicion_of_infection']);

    // Render acute orgdf deselection (step 2).
    var orgdf_label = 'Below, we list likely sources of organ dysfunction that triggered the alert. Remove any that you believe are not due to infection.';
    var orgdf_ctn = $('[data-trews="eval-acute-orgdf"]');
    orgdf_ctn.find('h3').text(orgdf_label);

    orgdf_ctn.removeClass('inactive highlight-expired highlight-unexpired');
    if ( this.susCtn.hasClass('highlight-expired') ) {
      orgdf_ctn.addClass('highlight-expired');
    }
    else if ( this.susCtn.hasClass('highlight-unexpired') ) {
      orgdf_ctn.addClass('highlight-unexpired');
    }

    // Show CMS Org DF criteria when TREWS Org DF has not been deactivated (for UI stability)
    // or when cms is the exclusive cause of alerting or sepsis onset.
    var only_cms_alerting = !trews_alerting && cms_alerting;
    var only_cms_is_sepsis = !trews.data['severe_sepsis']['is_trews'] && trews.data['severe_sepsis']['is_cms'];

    var alert_only_as_cms = only_cms_alerting || only_cms_is_sepsis;
    var not_acute_orgdf_only_as_cms = !trews_alerting && ( trews.data['severe_sepsis']['sirs']['is_met'] && cms_org_ovr );
    var orgdf_as_cms = !trews_org_ovr && (not_acute_orgdf_only_as_cms || alert_only_as_cms);

    this.orgSlot.r(orgdf_as_cms ? json['organ_dysfunction'] : json['trews_organ_dysfunction'],
                   orgdf_as_cms ? severe_sepsis['organ_dysfunction'] : severe_sepsis['trews_organ_dysfunction'],
                   orgdf_as_cms ? severe_sepsis['organ_dysfunction']['criteria_mapping'] : severe_sepsis['trews_organ_dysfunction']['criteria_mapping']);

    var num_orgdf = orgdf_as_cms ? json['organ_dysfunction']['num_met'] : json['trews_organ_dysfunction']['num_met'];
    var num_orgdf_ovr = orgdf_as_cms ? json['organ_dysfunction']['num_overridden'] : json['trews_organ_dysfunction']['num_overridden'];

    var any_ui_ovr = trews.data['ui']['ui_severe_sepsis']['is_met']
                      || trews.data['ui']['ui_septic_shock']['is_met'];

    var alert_snoozed = trews.data['ui']['ui_deactivate']['is_met'];

    if ( (num_orgdf == 0 && num_orgdf_ovr == 0 && !any_ui_ovr) || alert_snoozed ) {
      orgdf_ctn.addClass('inactive');
    }


    // Bind no-infection button.
    this.noInfectionBtn.unbind();
    this.noInfectionBtn.removeClass('pressed');

    if (this.sus['value'] != 'No Infection') {
      this.noInfectionBtn.click(function(e) {
        $('#loading').addClass('waiting').spin(); // Add spinner to page
        var action = {
          "actionName": $(this).attr('data-trews'),
          "value": 'No Infection'
        };
        endpoints.getPatientData("suspicion_of_infection", action);
      });
    }
    else {
      this.noInfectionBtn.addClass('pressed');
    }

    // Render card subtitle.
    /*
    var sepsis_summary = patientSevereSepsisSummary();

    if ( sepsis_summary != null ) {
      var subTitleElem = this.ctn.find('.card-subtitle');
      if ( sepsis_summary.subtitle != null ) {
        subTitleElem.text(sepsis_summary.subtitle);
        if ( sepsis_summary.subtitle_expired ) { subTitleElem.addClass('workflow-expired'); }
        else { subTitleElem.removeClass('workflow-expired'); }
      }
      else{
        subTitleElem.text('');
        subTitleElem.removeClass('workflow-expired');
      }
    }
    */

    if (trews.data.profile['deactivated'] || (workflowsComponent.sev36Override || workflowsComponent.sep6Override) ) {
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
    $('#expand-ten'), true, false, false);

  this.fusSlot = new slotComponent(
    $("[data-trews='fusion']"),
    $('#expand-fus'), true, false, false);

  this.render = function(json, severeSepsis) {
    this.ctn.find('h2.card-header').text(septic_shock['display_name']);

    if (json['is_met']) {
      this.ctn.addClass('complete');
    } else {
      this.ctn.removeClass('complete');
    }

    this.tenSlot.r(json['hypotension'], septic_shock['tension']);
    this.fusSlot.r(json['hypoperfusion'], septic_shock['fusion']);

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
      fnoteBtnText = "Clinically Inappropriate";
    }

    if ( fnoteBtnText ) {
      this.fnoteBtn.text(fnoteBtnText);
      var action = {
        "actionName": 'crystalloid_fluid'
      };

      if ( fnoteBtnText == 'Clinically Inappropriate' ) {
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

    // Render card subtitle.
    /*
    var shock_summary = patientSepticShockSummary();

    if ( shock_summary != null && shock_summary.subtitle != null ) {
      var subTitleElem = this.ctn.find('.card-subtitle');
      subTitleElem.text(shock_summary.subtitle);
      if ( shock_summary.subtitle_expired ) { subTitleElem.addClass('workflow-expired'); }
      else { subTitleElem.removeClass('workflow-expired'); }
    }
    */

    if ( trews.data.profile['deactivated'] || !severeSepsis || workflowsComponent.sep6Override ) {
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

  this.sev36Override = false;
  this.sep6Override = false;

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

      if ( release == 'epic2017' ) {
        // TODO: should we remove specific orders before posting, to implement replacement order semantics?
        appendToConsole('EPIC 2017 ORDER: ' + order + ' KEY: ' + key);
        if ( window.parent != null ) {
          window.parent.postMessage({
            'token': epicToken,
            'action': 'Epic.Clinical.Informatics.Web.PostOrder',
            'args': { 'OrderKey': key }
          }, '*');
          endpoints.getPatientData('place_order', {'actionName': order});
        } else {
          appendToConsole('Skipping order (null parent)');
        }
      }
      else {
        var txt = $(this).get()[0].innerHTML;
        var anc = $(this).find('a').first().get()[0];
        if ( anc != null ) { txt += '\nANCHOR:' + anc.innerHTML; }
        appendToConsole(txt);

        if ( anc != null ) {
          anc.click();
          endpoints.getPatientData('place_order', {'actionName': order});
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


  this.render = function(aJSON, bJSON, fJSON, iJSON, rJSON, vJSON, uiJSON, severeOnset, shockOnset) {
    // this.clean();
    this.sev3Ctn.find('h2[data-trews="sev3-header"]').text(workflows['sev3']['display_name']);
    this.sev6Ctn.find('h2').text(workflows['sev6']['display_name']);
    this.sep6Ctn.find('h2[data-trews="sep6-header"]').text(workflows['sep6']['display_name']);

    this.sev36Override = uiJSON['ui_severe_sepsis']['is_met'];
    this.sep6Override = uiJSON['ui_septic_shock']['is_met'];

    if ( (trews.data.profile['deactivated'] || severeOnset == null) && !(this.sev36Override || this.sep6Override) ) {
      this.sev3Ctn.addClass('inactive');
      this.sev6Ctn.addClass('inactive');
    } else {
      this.sev3Ctn.removeClass('inactive');
      this.sev6Ctn.removeClass('inactive');
    }

    if ( (trews.data.profile['deactivated'] || shockOnset == null) && !this.sep6Override ) {
      this.sep6Ctn.addClass('inactive');
    } else {
      this.sep6Ctn.removeClass('inactive');
    }

    var sev3LastOrder = Math.max(iJSON['time'], bJSON['time'], aJSON['time'], fJSON['time']);
    var sev3Complete = ( iJSON['is_met'] && orderStatusCompleted(iJSON) ) &&
                       ( bJSON['is_met'] && orderStatusCompleted(bJSON) ) &&
                       ( aJSON['is_met'] && orderStatusCompleted(aJSON) ) &&
                       ( fJSON['is_met'] && orderStatusCompleted(fJSON) );

    var sev6LastOrder = Math.max(sev3LastOrder, rJSON['time'])
    var sev6Complete = sev3Complete && (rJSON['is_met'] || orderStatusCompleted(rJSON));

    var shk6LastOrder = Math.max(sev6LastOrder, vJSON['time'])
    var shk6Complete = sev6Complete && (vJSON['is_met'] && orderStatusCompleted(vJSON) );

    this.sev3Ctn.find('.card-subtitle').html(this.workflowStatus('sev3', severeOnset, sev3LastOrder, sev3Complete));
    this.sev6Ctn.find('.card-subtitle').html(this.workflowStatus('sev6', severeOnset, sev6LastOrder, sev6Complete));
    this.sep6Ctn.find('.card-subtitle').html(this.workflowStatus('sep6', shockOnset, shk6LastOrder, shk6Complete));

    if ( sev3Complete && sev6Complete ) {
      $('.slot[data-trews="sus"] .numberCircle').addClass('complete');
      $('.slot[data-trews="eval-acute-orgdf"] .numberCircle').addClass('complete');
      $('.card[data-trews="sev3"] .numberCircle').addClass('complete');
    } else {
      $('.slot[data-trews="sus"] .numberCircle').removeClass('complete');
      $('.slot[data-trews="eval-acute-orgdf"] .numberCircle').removeClass('complete')
      $('.card[data-trews="sev3"] .numberCircle').removeClass('complete');
    }

    if ( shk6Complete ) {
      $('.card[data-trews="septicShock"] .numberCircle').addClass('complete');
      $('.card[data-trews="sep6"] .numberCircle').addClass('complete');
    } else {
      $('.card[data-trews="septicShock"] .numberCircle').removeClass('complete');
      $('.card[data-trews="sep6"] .numberCircle').removeClass('complete');
    }

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
/*
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
      if ( trews.data.profile['admit_time'] != null) {
        this.xmin = trews.data.profile['admit_time'] * 1000;
        this.xmax = ((Date.now() - this.xmin) / 6) + Date.now();
      } else {
        this.xmin = (Date.now() * 1000) - (6 * 60 * 60 * 1000)
        this.xmax = (Date.now() * 1000) + (6 * 60 * 60 * 1000)
      }
    }
    // Fixed range.
    // this.ymin = 0;
    // this.ymax = 1;

    // Two-sided dynamic range to fit data.
    // this.ymin = Math.min.apply(null, json['chart_values']['trewscore']);
    // this.ymin = this.ymin - (this.ymin * .03);
    // this.ymin = (this.ymin > json.trewscore_threshold) ? json.trewscore_threshold - 0.1 : this.ymin;
    // this.ymax = Math.max.apply(null, json['chart_values']['trewscore']) * 1.03;
    // this.ymax = (this.ymax < json.trewscore_threshold) ? json.trewscore_threshold + 0.1 : this.ymax;

    // One-sided dynamic range (upper) to fit data.
    this.ymin = 0;
    this.ymax = Math.max.apply(null, json['chart_values']['trewscore']) * 1.03;
    this.ymax = (this.ymax < json.trewscore_threshold) ? json.trewscore_threshold + 0.1 : this.ymax;

    graph(json, severeOnset, shockOnset, this.xmin, this.xmax, this.ymin, this.ymax);
  }
}

function graph(json, severeOnset, shockOnset, xmin, xmax, ymin, ymax) {
  var graphWidth = Math.floor($('#graph-wrapper').width()) - 10;
  $("#graphdiv").width(graphWidth);
  $("#graphdiv").height(graphWidth * .3225);
  $("#graphdiv").css('line-height', Number(graphWidth * .3225).toString() + 'px');
  var placeholder = $("#graphdiv");

  // NOTE: chart-only disabling is deprecated in favor of whole-page disabling.
  var disableChart = false; // json['patient_age'] < 18;

  if ( disableChart ) {
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
    {color: "#ccc", lineWidth: 1, xaxis: {from: xlast,to: xlast}},
    {color: "#e64535", lineWidth: 1, yaxis: {from: json['trewscore_threshold'],to: json['trewscore_threshold']}}
  ]

  var arrivalx = (trews.data.profile['admit_time'] != undefined) ? trews.data.profile['admit_time'] * 1000 : null;
  var severeOnsetx = (severeOnset != undefined) ? severeOnset * 1000 : null;
  var shockOnsetx = (shockOnset != undefined) ? shockOnset * 1000 : null;

  var severeOnsety = null;
  var shockOnsety = null;

  if (trews.data.profile['admit_time'] != undefined) {
    var arrivalMark = {color: "#ccc", lineWidth: 1, xaxis: {from: arrivalx,to: arrivalx}};
    //var arrivaly = json['chart_values']['trewscore'].indexOf(arrivalx);
    var arrivaly = jQuery.inArray(arrivalx, json['chart_values']['trewscore'])
    verticalMarkings.push(arrivalMark);
  }
  if (severeOnset != undefined) {
    var severeMark = {color: "#ccc", lineWidth: 1, xaxis: {from: severeOnsetx,to: severeOnsetx}};
    //severeOnsety = json['chart_values']['trewscore'].indexOf(severeOnsetx);
    severeOnsety = jQuery.inArray(severeOnsetx, json['chart_values']['trewscore'])
    verticalMarkings.push(severeMark);
  }
  if (shockOnset != undefined) {
    var shockMark = {color: "#ccc", lineWidth: 1, xaxis: {from: shockOnsetx,to: shockOnsetx}};
    //shockOnsety = json['chart_values']['trewscore'].indexOf(shockOnsetx);
    shockOnsety = jQuery.inArray(shockOnsetx, json['chart_values']['trewscore'])
    verticalMarkings.push(shockMark);
  }

  var maxYTick = (Math.floor(ymax / 0.05) * 0.05).toFixed(2);

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
      ticks: [[0, "0"], [json['trewscore_threshold'], json['trewscore_threshold']], [maxYTick, maxYTick]],
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

      // Note: no contributing features available in TREWS-JIT.
      //
      //var accessIndex = dataIndex >= json['chart_values']['tf_1_name'].length ?
      //                    json['chart_values']['tf_1_name'].length - 1 : dataIndex;

      //features += "<div class='row cf'>\
      //        <h4 class='name'>" + humanReadable(json['chart_values']['tf_1_name'][accessIndex]) + "</h4>\
      //        <h4 class='value'>" + json['chart_values']['tf_1_value'][accessIndex] + "</h4>\
      //      </div><div class='row cf'>\
      //        <h4 class='name'>" + humanReadable(json['chart_values']['tf_2_name'][accessIndex]) + "</h4>\
      //        <h4 class='value'>" + json['chart_values']['tf_2_value'][accessIndex] + "</h4>\
      //      </div><div class='row cf'>\
      //        <h4 class='name'>" + humanReadable(json['chart_values']['tf_3_name'][accessIndex]) + "</h4>\
      //        <h4 class='value'>" + json['chart_values']['tf_3_value'][accessIndex] + "</h4>\
      //      </div>";

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

  // Add deterioration risk label.
  var lo = plot.pointOffset({ x: xlast, y: json['trewscore_threshold']});
  placeholder.append("<div id='threshold' style='left:" + lo.left + "px; top: " + lo.top + "px;'>\
      <h3>High Risk<br/>for Deterioration</h3>\
      </div>");

  // Align to label bottom.
  var lbl = placeholder.find('#threshold');
  var lblTop = parseInt(lbl.css('top'), 10);
  var lblHeight = parseInt(lbl.css('height'), 10);
  lbl.css('top', (lblTop - lblHeight - 10) + 'px');
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

  // Align left boundary of placeholders.
  var placeholderLeft = parseInt($('#' + id).css('width'), 10) / 2;
  placeholder.find('#' + id + '.graph-tag').css('left', (placeholderLeft + o.left) + 'px');
}
*/


/**
 * Criteria Component
 * @param JSON String
 * @return {String} html for a specific criteria
 */
var criteriaComponent = function(c, constants, key, hidden, criteria_mapping, criteria_source, skip_threshold_and_value) {
  this.isOverridden = false;
  this.status = "";
  this.name = c['name'];
  this.criteria_mapping = criteria_mapping;
  this.criteria_source = criteria_source;
  this.criteria_button = null;
  this.criteria_button_enable = false;

  // Rendering variables.
  var hiddenClass = "";
  var deactivatedClass = "";

  var displayValue = c['value'];
  var displayBaselineValue = null;
  var fixed = constants['fixed'] == undefined ? null : constants['fixed'];
  var precision = constants['precision'] == undefined ? 5 : constants['precision'];

  // Baseline.
  if ( constants.baseline_key != null && constants.baseline_trend != null
          && trews.data['orgdf_baselines'][constants.baseline_key] != null )
  {
    displayBaselineValue = trews.data['orgdf_baselines'][constants.baseline_key];
  }

  // Handle TREWS organ dysfunction values as JSON objects.
  if ( c['name'].startsWith('trews_') && displayValue != null ) {
    try {
      var jsonValue = JSON.parse(displayValue);
      if ( isNumber(jsonValue) ) {
        displayValue = jsonValue;
      }
      else if ( 'value' in jsonValue ) {
        displayValue = jsonValue.value
      }
      else {
        displayValue = 'N/A';
      }

      if ( 'baseline' in jsonValue ) {
        displayBaselineValue = (jsonValue.baseline == null || jsonValue.baseline < 0) ? null : jsonValue.baseline;
      }
    } catch(e) {
      displayValue = 'N/A';
      displayBaselineValue = null;
    }
  }

  if ( displayValue && ( isNumber(displayValue) || !isNaN(Number(displayValue)) ) ) {
    if ( fixed == null ) {
      displayValue = Number(displayValue).toPrecision(precision);
    } else {
      displayValue = Number(displayValue).toFixed(fixed);
    }
  }

  if ( displayBaselineValue && ( isNumber(displayBaselineValue) || !isNaN(Number(displayBaselineValue)) ) ) {
    if ( fixed == null ) {
      displayBaselineValue = Number(displayBaselineValue).toPrecision(precision);
    } else {
      displayBaselineValue = Number(displayBaselineValue).toFixed(fixed);
    }
  }

  // Local conversions.
  if ( c['name'] == 'sirs_temp' && displayValue != null
        && ( isNumber(displayValue) || !isNaN(Number(displayValue)) ) )
  {
    displayValue = ((Number(displayValue) - 32) / 1.8).toPrecision(3);
  }

  if  ( (c['name'] == 'platelet' || c['name'] == 'trews_platelet') && displayValue != null
          && ( isNumber(displayValue) || !isNaN(Number(displayValue)) ) )
  {
    // Scale platelets (stored in units of 1,000).
    displayValue = (Number(displayValue) * 1000).toLocaleString();
    if ( displayBaselineValue != null && ( isNumber(displayBaselineValue) || !isNaN(Number(displayBaselineValue)) ) ) {
      displayBaselineValue = (Number(displayBaselineValue) * 1000).toLocaleString();
    }
  }

  if ( c['is_met'] && (c['name'] == 'respiratory_failure' || c['name'] == 'trews_vent') ) {
    displayValue = 'Mechanical Support: On';
  }

  if ( c['is_met'] && c['name'] == 'trews_vasopressors') {
    displayValue = 'Vasopressors: Initiated';
  }

  if (c['override_user'] != null) {
    this.isOverridden = true;
  }

  if (c['is_met'] && c['measurement_time']) {
    var lapsed = timeLapsed(new Date(c['measurement_time']*1000));
    var strTime = strToTime(new Date(c['measurement_time']*1000));

    if (c['name'] == 'trews_vasopressors') {
      this.status += "Vasopressors given <span title='" + strTime + "'>" + lapsed + "</span>";
    }
    else if (c['name'] == 'respiratory_failure' || c['name'] == 'trews_vent') {
      this.status += (this.criteria_source ? this.criteria_source + ' ' : '') + "Criteria met <span title='" + strTime + "'>" + lapsed + "</span>"
        + (skip_threshold_and_value ? '' : " with <span class='value'>" + displayValue + "</span>");
    }
    else {
      this.status += (this.criteria_source ? this.criteria_source + ' ' : '') + "Criteria met <span title='" + strTime + "'>" + lapsed + "</span>"
        + (skip_threshold_and_value ? '' :  " with a value of <span class='value'>" + displayValue + "</span>");
    }

    // Baseline.
    if ( constants.baseline_key != null && constants.baseline_trend != null
            && trews.data['orgdf_baselines'][constants.baseline_key] != null )
    {
      this.status += ', ' + constants.baseline_trend + ' from last recorded baseline of '
                  + displayBaselineValue + ' ' + constants.overrideModal[0].units;
    }
    this.status += (c['override_time']) ? "<br />" : "";
  }

  if (c['override_user'] != null && c['override_time']) {
    var oLapsed = timeLapsed(new Date(c['override_time']*1000));
    var oStrTime = strToTime(new Date(c['override_time']*1000));
    this.status += "Customized by " + c['override_user'] + " <span title='" + oStrTime + "'>" + oLapsed + "</span>";
  }

  if (c['is_met'] && (c['measurement_time'] || this.isOverridden)) {
    this.classComplete = " met";
  } else {
    this.classComplete = " unmet";
    if (c['override_user'] == null) {
      hiddenClass = (hidden) ? " hidden" : " unhidden";
    }
  }


  var criteriaString = "";
  var criteriaStringSuffix = "";
  var oCriteria = this.isOverridden ? trews.getSpecificCriteria(key, constants.key) : null;

  for (var i = 0; i < constants.overrideModal.length; i++) {
    var crit = null;
    if (oCriteria != null && i < oCriteria.override_value.length) {
      if (oCriteria.override_value[i] != undefined) {
        var oVal = oCriteria.override_value[i];
        if ( 'text' in oVal ) {
          crit = oVal.text;
        } else if (oVal.range == 'min' || oVal.range == 'max') {
          crit = oVal.lower ? oVal.lower : oVal.upper;
        } else {
          crit = [oVal.lower, oVal.upper]
        }
      }
    } else {
      crit = (constants.overrideModal[i].value != null) ? constants.overrideModal[i].value : constants.overrideModal[i].values
    }
    var name = constants.overrideModal[i].name
    var unit = constants.overrideModal[i].units

    // Patch precision.
    if ( c['name'] == 'sirs_temp' ) {
      crit[0] = Number(crit[0]).toPrecision(3);
      crit[1] = Number(crit[1]).toPrecision(3);
    }

    // Generate criteria string.
    if ( crit === 'No Infection' ) {
      deactivatedClass = " met-deactivated";
      criteriaString += name;
      criteriaStringSuffix = " measurements not due to infection";
      this.criteria_button_enable = true;
    }
    else if ( skip_threshold_and_value ) {
      criteriaString += name;
    }
    else if ( c['name'] == 'respiratory_failure' || c['name'] == 'trews_vent' ) {
      if (c['is_met']) {
        criteriaString += name;
      } else {
        var with_support = crit == 0 ? 'Off' : 'On';
        criteriaString += name + ": Mechanical Support: " + with_support;
      }
    }
    else if ( c['name'] == 'trews_vasopressors' ) {
      if (c['is_met']) {
        criteriaString += name;
      } else {
        var with_support = crit == 0 ? 'Not administered' : 'Initiated';
        criteriaString += name + ": " + with_support;
      }
    }
    else if (constants.overrideModal[i].range == 'true') {
      criteriaString += name + " < " + crit[0] + ' ' + unit + " or > " + crit[1] + ' ' + unit;
    }
    else {
      var comp = (constants.overrideModal[i].range == 'min') ? "<" : ">";
      criteriaString += name + " " + comp + " " + crit + ' ' + unit;
    }
    criteriaString += skip_threshold_and_value ? " / " : " or ";
  }

  if ( constants.baseline_key != null
        && constants.baseline_display_name != null
        && trews.data['orgdf_baselines'][constants.baseline_key] != null )
  {
    criteriaStringSuffix = ' and at least ' + constants.baseline_display_name + ' from baseline';
  }

  criteriaString = skip_threshold_and_value ? criteriaString.slice(0, -3) : criteriaString.slice(0, -4);
  criteriaString += criteriaStringSuffix;

  // Add criteria button (for organ dysfunction).
  var criteria_button_symbol = this.criteria_button_enable ? '<p>Re-enable</p>' : '<i class="fa fa-close"></i>';
    // '<i class="fa ' + (this.criteria_button_enable ? 'fa-check' : 'fa-close') + '"></i>';

  var cb_tooltip = this.criteria_button_enable ?
    'This organ dysfunction has been marked as not caused by infection. Click to re-enable (it will reset in 72 hours).'
    : 'Click to indicate that this organ dysfunction is not caused by infection, and disable. It will reset in 72 hours.'

  var data_src = this.criteria_mapping != null && this.criteria_mapping.src != null ?
    ('data-criteria-src="' + this.criteria_mapping.src + '" ') : '';

  var data_dst = this.criteria_mapping != null && this.criteria_mapping.dst != null ?
    ('data-criteria-dst="' + this.criteria_mapping.dst + '" ') :  '';

  this.criteria_button = (this.criteria_mapping == null || !(c['is_met'] || this.criteria_button_enable)) ? null :
    '<div style="float: right;">' +
      '<span class="criteria-btn" ' +
        'data-toggle="tooltip" title="' + cb_tooltip + '" ' +
        'data-as-enable="' + this.criteria_button_enable.toString() + '"' +
        data_src + data_dst + '>' +
      criteria_button_symbol + '</span></div>';

  this.html = "<div class='status" + this.classComplete + hiddenClass + deactivatedClass + "' data-trews='criteria_" + this.name + "'>\
          " + (this.criteria_button == null ? '' : this.criteria_button) + "\
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
  elem.removeClass('in-action in-progress discontinued expired complete not-needed');

  var obj_with_status = null;
  try {
    obj_with_status = JSON.parse(json['status']);
    if ( !('status' in obj_with_status) ) {
      obj_with_status = json;
    }
  } catch (e) {
    obj_with_status = json;
  }

  if ( obj_with_status != null ) {
    var status_completed = json['is_met'] && orderStatusCompleted(obj_with_status);
    if ( obj_with_status['status'] == 'Ordering' ) {
      elem.addClass('in-action');
    }
    else if ( obj_with_status['status'] == 'Ordered' ) {
      elem.addClass('in-progress');
    }
    /*
    // Yanif: disabling discontinued/ended for now based on dt's feedback on 11/1/17.
    else if ( obj_with_status['status'] == 'Discontinued' ) {
      elem.addClass('discontinued');
    }
    else if ( obj_with_status['status'] == 'Ended' ) {
      elem.addClass('expired');
    }
    */
    else if ( status_completed ) {
      elem.addClass('complete');
    }

    // For repeat lactates, append 'not needed' to its status as appropriate.
    if ( json['name'] == 'repeat_lactate_order' && json['is_met'] && !elem.is('.in-action,.in-progress,.complete') ) {
      elem.addClass('not-needed');
    }

    // For vasopressors, also append 'not needed' as appropriate.
    var vasopressors_met = json['name'] == 'vasopressors_order' && json['is_met'];

    var shock_met = trews.data['septic_shock']['is_met'] != null && trews.data['septic_shock']['is_met'];

    var no_hypotension = trews.data['septic_shock']['hypotension']['is_met'] != null
                          && !trews.data['septic_shock']['hypotension']['is_met'];

    if ( vasopressors_met && shock_met && no_hypotension && !elem.is('.in-action,.in-progress,.complete') ) {
      elem.addClass('not-needed');
    }

    // Add clinically inappropriate reason.
    var naMsg = '';
    var naPrefix = 'Clinically Inappropriate';
    if ( obj_with_status['status'] != null && obj_with_status['status'].startsWith(naPrefix) ) {
      naMsg = naPrefix;
      if (obj_with_status['status'].length > naPrefix.length + 1) {
        naMsg += ': ' + obj_with_status['status'].substr(naPrefix.length + 1);
      }
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
          endpoints.getPatientData('get_antibiotics');
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

  // Suppressions.
  this.suppressions = null;
  this.suppressionExpanded = false;

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
    var suppressed = false;

    if ( data['alert_code'] == '301' || data['alert_code'] == '304'
          || data['alert_code'] == '501' || data['alert_code'] == '504'
          || data['alert_code'] == '701' || data['alert_code'] == '704' )
    {
      var n = trews.getIncompleteSevereSepsis3hr();
      if ( n > 0 ) { alertMsg = String(n) + ' ' + alertMsg; } else { return null; }
    }
    else if ( data['alert_code'] == '302' || data['alert_code'] == '305'
                || data['alert_code'] == '502' || data['alert_code'] == '505'
                || data['alert_code'] == '702' || data['alert_code'] == '705' )
    {
      var n = trews.getIncompleteSevereSepsis6hr();
      if ( n > 0 ) { alertMsg = String(n) + ' ' + alertMsg; } else { return null; }
    }
    else if ( data['alert_code'] == '303' || data['alert_code'] == '306'
                || data['alert_code'] == '503' || data['alert_code'] == '506'
                || data['alert_code'] == '703' || data['alert_code'] == '706' )
    {
      var n = trews.getIncompleteSepticShock();
      if ( n > 0 ) { alertMsg = String(n) + ' ' + alertMsg; } else { return null; }
    }

    if ( data['alert_code'] == '206' || data['alert_code'] == '307'
            || data['alert_code'] == '406' || data['alert_code'] == '507' )
    {
      suppressed = true;
    }
    return {'msg': alertMsg, 'suppressed': suppressed};
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
    var numSuppressed = 0;
    var renderTs = Date.now();

    var suppressions = $('<div class="suppressions"></div>');

    for (var i = 0; i < data.length; i++) {
      // Skip notifications scheduled in the future, as part of the Dashan event queue.
      var notifTs = new Date(data[i]['timestamp'] * 1000);
      if (notifTs > renderTs) { continue; }

      // Skip messages if there is no content (used to short-circuit empty interventions).
      var notifResult = this.getAlertMsg(data[i]);
      if(notifResult == null)
      {
        // bundle has been completed so we can skip the alerts
        continue;
      }
      var notifMsg = notifResult['msg'];
      var notifSuppressed = notifResult['suppressed'];
      if ( notifMsg == undefined ) { continue; }

      // Display the notification.
      var notif = $('<div class="notification"></div>');
      notif.append('<h3>' + notifMsg + '</h3>')
      var subtext = $('<div class="subtext cf"></div>');
      subtext.append('<p>' + timeLapsed(new Date(data[i]['timestamp']*1000)) + '</p>');

      if ( notifSuppressed ) {
        numSuppressed++;
        notif.addClass('suppressed');
        notif.append(subtext);
        suppressions.prepend(notif);
      }
      else {
        /*
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
        */
        numUnread++; // TODO: remove when re-enabling mark-as-read functionality.
        notif.append(subtext);
        this.n.prepend(notif);
      }
    }

    if ( numSuppressed > 0 ) {
      // Since we have rebuilt the suppression items, remove any existing
      // items from the DOM and set the object variable in preparation for syncing.
      $('.suppressions').remove();
      this.suppressions = suppressions;

      // Add suppression summary link.
      var suppressionSummary = $('<div class="suppression-summary"></div>');
      var descPluralized = numSuppressed == 1 ? 'suppressed message' : 'suppressed messages';
      var expanderMsg = this.suppressionExpanded ? '(minimize)' : '(see all)';
      var expanderCls = this.suppressionExpanded ? 'unhidden' : 'hidden';
      var expanderHtml = '<span class="expander ' + expanderCls + '">' + expanderMsg + '</span>';
      suppressionSummary.append('<h3>' + numSuppressed + ' ' + descPluralized + ' ' + expanderHtml + '</h3>');

      // Add expander handler.
      var expander = suppressionSummary.find('.expander');
      expander.unbind();
      expander.click(function(e) {
        e.stopPropagation();
        if ( $(this).hasClass('hidden') ) {
          $(this).text('(minimize)').removeClass('hidden').addClass('unhidden');
          $('#notifications').append(notifications.suppressions);
          notifications.suppressionExpanded = true;
        } else {
          notifications.suppressionExpanded = false;
          $('#notifications .suppressions').remove();
          $(this).text('(see all)').removeClass('unhidden').addClass('hidden');
        }
      });

      // Add the summary and if necessary, suppression items.
      this.n.append(suppressionSummary);
      if ( this.suppressionExpanded ) { this.n.append(this.suppressions); }
    }

    // Highlight next step if we have a code 300, and code 205 has not passed.
    var susCtn = $("[data-trews='sus']");
    if ( !susCtn.hasClass('complete') || !susCtn.hasClass('complete-with-status') ) {
      var highlightCls = severeSepsisComponent.highlightSuspicionClass();
      if ( highlightCls != null ) {
        susCtn.addClass(highlightCls);
        susCtn.find('.status').show();
        susCtn.find('.status h4').text('');
        //susCtn.find('.status h4').text('Please indicate whether infection is suspected');
      } else {
        susCtn.removeClass('highlight-expired highlight-unexpired');
      }
    } else {
      susCtn.removeClass('highlight-expired highlight-unexpired');
    }

    if ( trews.data.profile['deactivated'] ) {
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

    var cms_orgdf = [ "blood_pressure",
                      "mean_arterial_pressure",
                      "decrease_in_sbp",
                      "respiratory_failure",
                      "creatinine",
                      "bilirubin",
                      "platelet",
                      "inr",
                      "lactate" ];

    var trews_orgdf = [ "trews_sbpm",
                        "trews_map",
                        "trews_dsbp",
                        "trews_vent",
                        "trews_creatinine",
                        "trews_bilirubin",
                        "trews_platelet",
                        "trews_inr",
                        "trews_lactate",
                        "trews_gcs",
                        "trews_vasopressors" ];

    var msg = "";
    var user = data.uid == null ? 'TREWS' : (data['uid'] == 'dba' ? 'TREWS automatically' : data['uid']);
    if ( user == 'user' ) { user = 'Test user'; }

    var is_order = jQuery.inArray(data.name, order_overrides) >= 0;
    var is_text = jQuery.inArray(data.name, text_overrides) >= 0;
    var is_cms_orgdf = jQuery.inArray(data.name, cms_orgdf) >= 0;
    var is_trews_orgdf = jQuery.inArray(data.name, trews_orgdf) >= 0;


    // SKip CMS orgdf entries since there's a matching TREWS orgdf entry in the log.
    if ( is_cms_orgdf ) { return null; }

    if (data['event_type'] == 'set_deterioration_feedback') {
      if (data.value.other == "" && data.value.value.length == 0) {
        return user + " has cleared <b>other conditions driving deterioration</b>"
      }
      msg += user + LOG_STRINGS[data['event_type']]
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
        if ( data.name == 'ui_severe_sepsis' || data.name == 'ui_septic_shock' ) {
          msg += user + " reset the <b>manual override</b> for the <b>" + criteriaKeyToName[data.name][0].name + "</b>";
        }
        else if ( criteriaKeyToName[data.name].length > 0 && is_trews_orgdf )
        {
          var event_type = null;
          if ( data.name == 'respiratory_failure' || data.name == 'trews_vent' ) {
            event_type = '<b>Respiratory Failure (Mechanical Ventilation)</b>';
          }
          else if ( data.name == 'trews_vasopressors' ) {
            event_type = '<b>Vasopressors</b>';
          }
          else {
            event_type = '<b>' + criteriaKeyToName[data.name][0].name + '</b> measurements';
          }
          msg += user + " re-enabled the entry for acute organ dysfunction based on " + event_type;
        }
        else {
          var action = data.name == 'suspicion_of_infection' ? " cleared " : LOG_STRINGS[data['event_type']]['clear'];
          msg += user + action;
          for (var i = 0; i < criteriaKeyToName[data.name].length; i ++) {
            var suffix = criteriaKeyToName[data.name].length > 1 ? ", " : "";
            // For the last element, add an 'and' to the rendered message,
            // stripping the comma if we have exactly 2 elements,
            if ( criteriaKeyToName[data.name].length > 1 && i == (criteriaKeyToName[data.name].length - 1) ) {
              if ( criteriaKeyToName[data.name].length == 2 ) {
                msg = msg.substring(0, msg.length - 2) + " "
              }
              msg += "and ";
              suffix = "";
            }
            msg += '<b>' + criteriaKeyToName[data.name][i].name + '</b>' + suffix;
          }
        }
      } else {
        if ( data.name == 'ui_severe_sepsis' || data.name == 'ui_septic_shock' ) {
          msg += user + " <b>manually overrode</b> the <b>" + criteriaKeyToName[data.name][0].name + "</b>";
        }
        else if ( data.override_value.length > 0 && criteriaKeyToName[data.name].length > 0
                    && data.override_value[0].text == 'No Infection' && is_trews_orgdf )
        {
          // Handle no acute organ dysfunction.
          var event_type = null;
          if ( data.name == 'respiratory_failure' || data.name == 'trews_vent' ) {
            event_type = '<b>Respiratory Failure (Mechanical Ventilation)</b> does';
          }
          else if ( data.name == 'trews_vasopressors' ) {
            event_type = '<b>Vasopressors</b>';
          }
          else {
            event_type = '<b>' + criteriaKeyToName[data.name][0].name + '</b> measurements do';
          }
          msg += user + " entered " + event_type + " not indicate organ dysfunction due to infection";
        }
        else {
          var action = is_order ? LOG_STRINGS[data['event_type']]['ordered'][0] : LOG_STRINGS[data['event_type']]['customized'][0];
          msg += user + action;
          for (var i = 0; i < criteriaKeyToName[data.name].length; i ++) {
            if ( i >= data.override_value.length ) {
              // Strip trailing comma
              if ( criteriaKeyToName[data.name].length > 1 ) { msg = msg.substring(0, msg.length - 2) + " " }
              break;
            }

            var suffix = criteriaKeyToName[data.name].length > 1 ? ", " : "";
            // For the last element, add an 'and' to the rendered message,
            // stripping the comma if we have exactly 2 elements,
            if ( criteriaKeyToName[data.name].length > 1 && i == (criteriaKeyToName[data.name].length - 1) ) {
              if ( criteriaKeyToName[data.name].length == 2 ) {
                msg = msg.substring(0, msg.length - 2) + " "
              }
              msg += "and ";
              suffix = "";
            }
            msg += '<b>' + criteriaKeyToName[data.name][i].name + '</b>';
            if ( data.override_value != null && data.override_value.length > i ) {
              if (is_order) {
                msg += LOG_STRINGS[data['event_type']]['ordered'][1] + data.override_value[i].text;
              }
              else if (is_text || data.override_value[i].text != null) {
                msg += LOG_STRINGS[data['event_type']]['customized'][1] + '<b>' + data.override_value[i].text + '</b>';
              }
              else if ( data.override_value[i].range != null ) {
                msg += LOG_STRINGS[data['event_type']]['customized'][1]
                      + UpperLowerToLogicalOperators(data.override_value[i], criteriaKeyToName[data.name][i].units);
              }
              msg += suffix;
            }
          }
        }
      }
    }
    else if (data['event_type'] == 'deactivate' || data['event_type'] == 'auto_deactivate') {
      event_type = data['deactivated'] ? 'deactivate' : 'activate';
      msg += user + LOG_STRINGS[event_type]
    }
    else {
      msg += user + LOG_STRINGS[data['event_type']]
    }
    return msg;
  }
  this.render = function(data) {
    this.a.html('');
    if (data == undefined) {
      this.a.append('<p class="none">Can\'t retrieve activity log at this time.  <br />Activity Log may be under construction.</p>')
      return;
    }
    if (data.length == 0) {
      this.a.append('<p class="none">No Activity</p>')
      return;
    }

    // Sort a deep copy of the activity log by descending timestamp.
    var activities = data.concat().sort(function(a,b) {
      return a['timestamp'] == b['timestamp'] ? 0 : (a['timestamp'] < b['timestamp'] ? -1 : 1);
    });

    var last_cms_expired_idx = null;
    for (var i = 0; i < activities.length; i++) {
      if ( activities[i]['event_type'] == 'reset_soi_pats' ) {
        last_cms_expired_idx = i;
      }
    }

    for (var i = 0; i < activities.length; i++) {
      var time = new Date(activities[i]['timestamp'] * 1000);

      // Skip CMS expiry entries except the last one.
      if ( activities[i]['event_type'] == 'reset_soi_pats' && i != last_cms_expired_idx ) { continue; }

      // Skip messages if there is no content (used to short-circuit empty interventions).
      var msg = this.getLogMsg(activities[i]);
      if ( msg == undefined ) { continue; }

      // Display the notification.
      var log = $('<div class="log-item"></div>');
      log.append('<h3>' + msg + '</h3>')
      var subtext = $('<div class="subtext cf"></div>');
      subtext.append('<p>' + timeLapsed(new Date(activities[i]['timestamp']*1000)) + '</p>');
      log.append(subtext);
      this.a.prepend(log);
    }
  }
}

/**
 * Toolbar.
 * This component manages all buttons on the toolbar (other than the notifications badge),
 * as well as the status header bar.
 */

var toolbar = new function() {
  this.resetNav = $('#header-reset-patient');
  this.activateNav = $('#header-activate-button');
  this.feedback = $('#feedback');
  this.feedbackSuccessHideDelay = 3500;
  this.statusBar = $('#status-header');
  this.disclaimerBar = $('#disclaimer-header');
  this.disclaimerTimeout = null;

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
      endpoints.getPatientData('deactivate', {'value': !trews.data.profile['deactivated']}, toolbar.activateNav);
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

    // Initialize disclaimer bar, and add timer to hide it.
    var dept = (getQueryVariable('DEP') === false) ? null : getQueryVariable('DEP');
    var dept_is_ed = dept == '110300460' || dept == '110300470';
    var disclaimer_enabled = false; // TODO: enable as needed
    if ( disclaimer_enabled && !dept_is_ed ) {
      this.disclaimerTimeout = window.setTimeout(function() {
        $('#disclaimer-header').fadeOut(1000, function() {
          $('#disclaimer-header').css('height', '0px');
          $('#disclaimer-header').css('border', '0');
          refreshHeaderHeight('disclaimer-timeout');
          toolbar.disclaimerTimeout = null;
        });
      }, 30*1000);
    }
  }
  this.feedbackSuccess = function() {
    this.feedback.find('p').html('<b class="success">Feedback Submitted!</b>')
    toolbar.feedback.find('textarea').val('')
    window.setTimeout(function() { toolbar.feedback.fadeOut(500); }, toolbar.feedbackSuccessHideDelay);
  }
  this.feedbackError = function() {
    this.feedback.find('p').html('<b class="error">There was an error, please try again or email trews-helpdesk@opsdx.io</b>')
  }
  this.render = function(json) {
    this.resetNav.show()
    if ( trews.data.profile['deactivated'] ) {
      this.activateNav.find('span').text('Activate Patient');
    } else {
      this.activateNav.find('span').text('Deactivate Patient');
    }
    this.activateNav.show()

    // Render status bar.
    // longPatientSummary args: with_alert, action_type, with_treatment, with_reset, with_no_risk
    var patient_summary = longPatientSummary(false, 2, true, true, false);

    if ( patient_summary.care_status ){
      this.statusBar.html('<h5>' + patient_summary.care_status + '</h5>');
      this.statusBar.show();
    } else {
      this.statusBar.html('');
      this.statusBar.hide();
    }

    this.statusBar.removeClass('high-priority low-priority no-priority');
    if ( patient_summary.care_status_priority != null ) {
      this.statusBar.addClass(patient_summary.care_status_priority);
    }

    // Render disclaimer bar.
    if ( this.disclaimerTimeout != null ) {
      var msg = '<b><u>Do NOT rely on TREWS for inpatient alerts.</u></b> '
                + 'The current version scores and alerts for patients only in the Emergency Department. '
                + 'The next version will target inpatients as well.';

      this.disclaimerBar.html('<h5>' + msg + '</h5>');
      this.disclaimerBar.show();
    }
  }
}


/**
 * Timeline viewer.
 */

 var timeline = new function() {
  this.chart = null;
  this.chartOptions = null;
  this.chartMin = null;
  this.chartMax = null;
  this.chartInitialized = false;
  this.chartHasSepsisMarker = false;
  this.groups = {};
  this.groupDataSet = new vis.DataSet();
  this.clsSegments = {};

  this.ctn = $('#timeline-wrapper');
  this.chartHdr = $('#timeline-header');
  this.chartCtn = $('#timeline-div');
  this.zoomCtn = $('#timeline-zoom-div');
  this.expandBtn = $('#expand-timeline');
  this.timelineVisible = false;

  this.startOfHour = function(d) {
    dStart = new Date(d);
    dStart.setUTCMinutes(0);
    dStart.setUTCSeconds(0);
    dStart.setUTCMilliseconds(0);
    return dStart.getTime();
  }

  this.startOfDay = function(d) {
    dStart = new Date(d);
    dStart.setUTCHours(0);
    dStart.setUTCMinutes(0);
    dStart.setUTCSeconds(0);
    dStart.setUTCMilliseconds(0);
    return dStart.getTime();
  }

  this.zoomToFit = function() {
    var hour = 3600 * 1000;
    var now = timeline.chart.getCurrentTime();
    var itemRange = timeline.chart.getItemRange();
    var minTime = itemRange.min == null ? now.getTime() - 6 * hour : itemRange.min.getTime();
    var maxTime = now.getTime();
    timeline.chart.setWindow(new Date(minTime - 2 * hour), new Date(maxTime + 2 * hour));
  }

  this.init = function() {
    var today = this.startOfDay(new Date()),
        day = 1000 * 60 * 60 * 24;

    var groupId = 30;
    var sirsGroupIds = [];
    var cmsOrganGroupIds = [];
    var trewsOrganGroupIds = [];
    var tensionGroupIds = [];
    var fusionGroupIds = [];

    for(var c in severe_sepsis['trews_organ_dysfunction']['criteria']) {
      var cr = severe_sepsis['trews_organ_dysfunction']['criteria'][c];
      var g = { id: groupId, content: cr['overrideModal'][0]['name'] };
      this.groups[cr['key']] = g;
      trewsOrganGroupIds.push(groupId);
      groupId++;
    }

    this.groups['trews_vasopressors'] = { id: groupId, content: 'Vasopressors' };
    trewsOrganGroupIds.push(groupId);
    groupId++;

    for(var c in severe_sepsis['sirs']['criteria']) {
      var cr = severe_sepsis['sirs']['criteria'][c];
      var g = { id: groupId, content: cr['overrideModal'][0]['name'] };
      this.groups[cr['key']] = g;
      sirsGroupIds.push(groupId);
      groupId++;
    }

    for(var c in severe_sepsis['organ_dysfunction']['criteria']) {
      var cr = severe_sepsis['organ_dysfunction']['criteria'][c];
      var g = { id: groupId, content: cr['overrideModal'][0]['name'] };
      this.groups[cr['key']] = g;
      cmsOrganGroupIds.push(groupId);
      groupId++;
    }

    for(var c in septic_shock['tension']['criteria']) {
      var cr = septic_shock['tension']['criteria'][c];
      var key = cr['key'] == 'hypotension_sbp' ? 'systolic_bp' : cr['key']; // HACK: Hard-coded key mapping for sbp
      var g = { id: groupId, content: cr['overrideModal'][0]['name'] };
      this.groups[key] = g;
      tensionGroupIds.push(groupId);
      groupId++;
    }

    for(var c in septic_shock['fusion']['criteria']) {
      var cr = septic_shock['fusion']['criteria'][c];
      var key = cr['key'] == 'initial_lactate' ?  'hpf_initial_lactate' : cr['key']; // HACK: deduplicate key with 'initial_lactate' for orders.
      var g = { id: groupId, content: cr['overrideModal'][0]['name'] };
      this.groups[key] = g;
      fusionGroupIds.push(groupId);
      groupId++;
    }

    groupId = 1;

    var trewsSepsisGroupId = groupId++;
    var trewsShockGroupId = groupId++;
    var cmsSepsisGroupId = groupId++;
    var cmsShockGroupId = groupId++;

    this.groups['suspicion_of_infection'] = { id: groupId++, className: 'vis_g_soi', content: 'Suspected Source Of Infection' };

    // Leaves for trews_severe_sepsis group.
    this.groups['trews_sevsep_soi']   = { id: groupId++, content: 'Suspected Source Of Infection' };
    this.groups['trews_sevsep_score'] = { id: groupId++, content: 'TREWS Acuity Score' };
    this.groups['trews_sevsep_org']   = { id: groupId++, content: 'TREWS Organ Dysfunction' };

    // Leaves for trews_septic_shock group
    this.groups['trews_sepshk_hypotension']   = {id: groupId++, content: 'TREWS Hypotension'};
    this.groups['trews_sepshk_hypoperfusion'] = {id: groupId++, content: 'TREWS Hypoperfusion'};

    // Leaves for cms_severe_sepsis group.
    this.groups['cms_soi']  = { id: groupId++, content: 'Suspected Source Of Infection' };
    this.groups['cms_sirs'] = { id: groupId++, content: 'CMS SIRS' };
    this.groups['cms_org']  = { id: groupId++, content: 'CMS Organ Dysfunction' };

    // Leaves for cms_septic_shock group.
    this.groups['cms_hypotension']   = {id: groupId++, content: 'CMS Hypotension'};
    this.groups['cms_hypoperfusion'] = {id: groupId++, content: 'CMS Hypoperfusion'};

    // TREWS Acuity Score
    this.groups['trewscore'] = { id: groupId++, className: 'vis_g_trewscore', content: 'TREWS Acuity Score' };

    // TREWS OrgDF hierarchy
    this.groups['trews_org'] = {
      id: groupId++,
      content: "TREWS Organ Dysfunction",
      nestedGroups: trewsOrganGroupIds
    };

    // SIRS hierarchy
    this.groups['sirs'] = {
      id: groupId++,
      content: "SIRS",
      nestedGroups: sirsGroupIds
    };

    // CMS OrgDF hierarchy
    this.groups['org'] = {
      id: groupId++,
      content: "CMS Organ Dysfunction",
      nestedGroups: cmsOrganGroupIds
    };

    // Hypotension hierarchy
    this.groups['tension'] = {
      id: groupId++,
      content: "Hypotension",
      nestedGroups: tensionGroupIds
    };

    // Hypoperfusion hierarchy
    this.groups['fusion'] = {
      id: groupId++,
      content: "Hypoperfusion",
      nestedGroups: fusionGroupIds
    };

    // Leaves for orders group.
    this.groups['blood_culture']     = { id: 100, className: 'vis_g_blood_culture',     content: 'Blood Culture'   };
    this.groups['initial_lactate']   = { id: 101, className: 'vis_g_initial_lactate',   content: 'Initial Lactate' };
    this.groups['crystalloid_fluid'] = { id: 102, className: 'vis_g_crystalloid_fluid', content: 'Fluid'           };
    this.groups['antibiotics']       = { id: 103, className: 'vis_g_antibiotics',       content: 'Antibiotics'     };
    this.groups['repeat_lactate']    = { id: 104, className: 'vis_g_repeat_lactate',    content: 'Repeat Lactate'  };
    this.groups['vasopressors']      = { id: 105, className: 'vis_g_vasopressors',      content: 'Vasopressors'    };

    var orderGroupIds = [100,101,102,103,104,105];

    this.groups['orders'] = {
      id: groupId++,
      content: "Orders",
      nestedGroups: orderGroupIds
    };

    // Add aggregated groups.
    this.groups['trews_severe_sepsis'] = {
      id: trewsSepsisGroupId,
      content: "TREWS Severe Sepsis",
      nestedGroups: [this.groups['trews_sevsep_soi']['id'], this.groups['trews_sevsep_score']['id'], this.groups['trews_sevsep_org']['id']]
    };

    this.groups['trews_septic_shock'] = {
      id: trewsShockGroupId,
      content: "TREWS Septic Shock",
      nestedGroups: [this.groups['trews_sepshk_hypotension']['id'], this.groups['trews_sepshk_hypoperfusion']['id']]
    };

    this.groups['cms_severe_sepsis'] = {
      id: cmsSepsisGroupId,
      className: 'vis_g_cms_severe_sepsis',
      content: "CMS Severe Sepsis",
      nestedGroups: [this.groups['cms_soi']['id'], this.groups['cms_sirs']['id'], this.groups['cms_org']['id']]
    };

    this.groups['cms_septic_shock'] = {
      id: cmsShockGroupId,
      content: "CMS Septic Shock",
      nestedGroups: [this.groups['cms_hypotension']['id'], this.groups['cms_hypoperfusion']['id']]
    };

    // create visualization
    this.chartMin = today - 7 * day;
    this.chartMax = today + 1.5 * day;
    this.chartOptions = {
      align: 'left',
      min: this.chartMin,
      max: this.chartMax,
      rollingMode: { follow: false, offset: 0.75 },
      selectable: false,
      stack: false,
      zoomable: false,
      //zoomMin: 60 * 1000,
      //zoomMax: 8 * 24 * 3600 * 1000,
      groupOrder: function (a, b) {
        return a.id - b.id;
      },
    };
    this.chart = new vis.Timeline(this.chartCtn[0], [], this.chartOptions);

    $('.timeline-zoom-btn').click(function(e) {
      e.stopPropagation();
      var hour = 3600 * 1000;
      if ( $(this).attr('data-zoom-hours').toLowerCase() == 'fit' ) {
        timeline.zoomToFit();
      } else {
        var now = timeline.chart.getCurrentTime();
        var lastHour = timeline.startOfHour(now);
        var lookback = parseInt($(this).attr('data-zoom-hours'), 10)
        timeline.chart.setWindow(new Date(lastHour - lookback * hour), new Date(lastHour + 5 * hour));
      }
    });

  }

  this.allOverlaps = function(cls, intervalsByCls, withSingletons, onPair) {
    // Array to hold all overlapping intervals.
    var overlaps = [];

    for (var i=0; i < intervalsByCls[cls].length; i++) {
      if ( withSingletons && intervalsByCls[cls].length == 1 ) {
        overlaps.push(intervalsByCls[cls][0]);
      }
      else {
        for (var j=i+1; j < intervalsByCls[cls].length; j++) {

          var overlap = !(intervalsByCls[cls][i].end < intervalsByCls[cls][j].start
                          || intervalsByCls[cls][j].end < intervalsByCls[cls][i].start);

          if ( overlap ) {
            overlaps.push(onPair(intervalsByCls[cls][i], intervalsByCls[cls][j]))
          }
        }
      }
    }

    return overlaps;
  }

  this.unionOverlap = function(i1, i2) {
    var start = new Date(Math.min(i1.start, i2.start));
    var end = new Date(Math.max(i1.end, i2.end));
    return {start: start, end: end };
  }

  this.intersectOverlap = function(i1, i2) {
    var start = new Date(Math.max(i1.start, i2.start));
    var end = new Date(Math.min(i1.end, i2.end));
    return {start: start, end: end };
  }

  this.pairwiseOverlapUnion = function(cls, intervalsByCls, withSingletons, onOverlap) {

    // Array to store disjoint segments resulting from simplification.
    var segments = [];

    // 1. Compute all overlaps.
    var overlaps = this.allOverlaps(cls, intervalsByCls, withSingletons, onOverlap);

    // 2. Simplify intervals to disjoint set.

    // Fixpoint simplification.
    while ( overlaps.length > 0 ) {
      // Pop, and simplify against all other array items.
      var o = overlaps.pop();

      // Indexes of segments for reducing 'o'.
      var reduced = [];

      // Cover segments that overlap with 'o', and track their indexes 'i' for removal.
      for (var i=0; i < overlaps.length; i++) {
        var reduce = !(o.end < overlaps[i].start || overlaps[i].end < o.start);
        if ( reduce ) {
          var start = new Date(Math.min(o.start, overlaps[i].start));
          var end = new Date(Math.max(o.end, overlaps[i].end));
          o = {start: start, end: end}

          // Add to the beginning of the reduced index for easy removal.
          reduced.unshift(i);
        }
      }

      // Remove all elements newly covered by 'o'.
      // Since reduced is in reverse index order, we can simply use splice to delete every index.
      if ( reduced.length > 0 ) {
        for (var i=0; i < reduced.length; i++) {
          overlaps.splice(reduced[i], 1);
        }

        // Add 'o' back to the overlaps for fixpoint simplification.
        overlaps.push(o);
      }
      else {
        // Otherwise this segment is disjoint, and should be added to the 'done' list.
        segments.push(o);
      }
    }

    return segments;
  }

  this.render = function(json) {

    var now = this.chart.getCurrentTime();

    // Compute onset and reset times for rendering.
    var range_as_point_duration = 10 * 60 * 1000;
    var deadline3_duration = 3  * 3600 * 1000;
    var deadline6_duration = 6  * 3600 * 1000;
    var reset_duration = (6+72) * 3600 * 1000;

    var severe_sepsis_by_trews = json['severe_sepsis']['is_trews']
    var severe_sepsis_by_cms = json['severe_sepsis']['is_cms']

    var severe_sepsis_start =
      ( json['severe_sepsis']['onset_time'] != null ) ?
        new Date(json['severe_sepsis']['onset_time'] * 1000) : null;

    var septic_shock_start =
      ( json['septic_shock']['onset_time'] != null ) ?
        new Date(json['septic_shock']['onset_time'] * 1000) : null;

    var severe_sepsis_deadline3 =
      severe_sepsis_start == null ? null : new Date(severe_sepsis_start.getTime() + deadline3_duration);

    var severe_sepsis_deadline6 =
      severe_sepsis_start == null ? null : new Date(severe_sepsis_start.getTime() + deadline6_duration);

    var septic_shock_deadline =
      septic_shock_start == null ? null : new Date(septic_shock_start.getTime() + deadline6_duration);

    var severe_sepsis_reset =
      severe_sepsis_start == null ? null : new Date(severe_sepsis_start.getTime() + reset_duration);

    var septic_shock_reset =
      septic_shock_start == null ? null : new Date(septic_shock_start.getTime() + reset_duration);

    var severe_sepsis_completed = { status: true, t: null };
    var septic_shock_completed = { status: true, t: null };


    // Calculate and save showNested state before reassignment.
    var allGroups = this.groupDataSet.get();
    var visibleGroups = this.groupDataSet.get({filter: function(g) { return g.visible; }});
    var visibleIds = $.map(visibleGroups, function(g) { return g.id; });
    var expandedParents = $.map(visibleGroups, function(g) { return g.nestedInGroup; });

    this.groupDataSet = new vis.DataSet();
    this.groupDataSet.add(this.groups['trewscore']);
    this.groupDataSet.add(this.groups['suspicion_of_infection']);

    // Add trews_severe_sepsis and its leaves.
    // We only color the TREWS and CMS groups if severe sepsis was due to the corresponding trigger.
    this.groupDataSet.add($.extend({}, this.groups['trews_sevsep_soi'],   { visible: visibleIds.indexOf(this.groups['trews_sevsep_soi']['id'])  >= 0 } ));
    this.groupDataSet.add($.extend({}, this.groups['trews_sevsep_score'],  { visible: visibleIds.indexOf(this.groups['trews_sevsep_score']['id']) >= 0 } ));
    this.groupDataSet.add($.extend({}, this.groups['trews_sevsep_org'],   { visible: visibleIds.indexOf(this.groups['trews_sevsep_org']['id'])  >= 0 } ));

    var trews_severe_sepsis_extension = { showNested: expandedParents.indexOf(this.groups['trews_severe_sepsis']['id']) >= 0 };
    if ( severe_sepsis_start != null && severe_sepsis_by_trews ) { trews_severe_sepsis_extension['className'] = 'vis_g_severe_sepsis_active'; }
    this.groupDataSet.add($.extend({}, this.groups['trews_severe_sepsis'], trews_severe_sepsis_extension));

    this.groupDataSet.add($.extend({}, this.groups['trews_sepshk_hypotension'],   { visible: visibleIds.indexOf(this.groups['trews_sepshk_hypotension']['id'])  >= 0 } ));
    this.groupDataSet.add($.extend({}, this.groups['trews_sepshk_hypoperfusion'], { visible: visibleIds.indexOf(this.groups['trews_sepshk_hypoperfusion']['id']) >= 0 } ));

    var trews_septic_shock_extension = { showNested: expandedParents.indexOf(this.groups['trews_septic_shock']['id']) >= 0 };
    if ( septic_shock_start != null && severe_sepsis_by_trews ) { trews_septic_shock_extension['className'] = 'vis_g_septic_shock_active'; }
    this.groupDataSet.add($.extend({}, this.groups['trews_septic_shock'], trews_septic_shock_extension));

    // Add cms_severe_sepsis and its leaves.
    this.groupDataSet.add($.extend({}, this.groups['cms_org'],  { visible: visibleIds.indexOf(this.groups['cms_org']['id'])  >= 0 } ));
    this.groupDataSet.add($.extend({}, this.groups['cms_soi'],  { visible: visibleIds.indexOf(this.groups['cms_soi']['id'])  >= 0 } ));
    this.groupDataSet.add($.extend({}, this.groups['cms_sirs'], { visible: visibleIds.indexOf(this.groups['cms_sirs']['id']) >= 0 } ));

    var cms_severe_sepsis_extension = { showNested: expandedParents.indexOf(this.groups['cms_severe_sepsis']['id']) >= 0 };
    if ( severe_sepsis_start != null && severe_sepsis_by_cms ) { cms_severe_sepsis_extension['className'] = 'vis_g_severe_sepsis_active'; }
    this.groupDataSet.add($.extend({}, this.groups['cms_severe_sepsis'], cms_severe_sepsis_extension));

    this.groupDataSet.add($.extend({}, this.groups['cms_hypotension'],   { visible: visibleIds.indexOf(this.groups['cms_hypotension']['id'])  >= 0 } ));
    this.groupDataSet.add($.extend({}, this.groups['cms_hypoperfusion'], { visible: visibleIds.indexOf(this.groups['cms_hypoperfusion']['id']) >= 0 } ));

    var cms_septic_shock_extension = { showNested: expandedParents.indexOf(this.groups['cms_septic_shock']['id']) >= 0 };
    if ( septic_shock_start != null && severe_sepsis_by_cms ) { cms_septic_shock_extension['className'] = 'vis_g_septic_shock_active'; }
    this.groupDataSet.add($.extend({}, this.groups['cms_septic_shock'], cms_septic_shock_extension));

    // create a data set with items
    var items = new vis.DataSet();

    // Constants.
    var criteriaClasses = [
      {'cls': 'sirs'                    , 'pcls' : 'severe_sepsis'},
      {'cls': 'organ_dysfunction'       , 'pcls' : 'severe_sepsis'},
      // {'cls': 'trews_organ_dysfunction' , 'pcls' : 'severe_sepsis'},
      {'cls': 'hypotension'             , 'pcls' : 'septic_shock'},
      {'cls': 'hypoperfusion'           , 'pcls' : 'septic_shock'}
    ];

    var greyItemStyle      = 'background-color: #555; color: #fff; border-color: #555;';
    var midGreyItemStyle   = 'background-color: #777; color: #fff; border-color: #777;';
    var redItemStyle       = 'background-color: #BF0F00; color: #fff; border-color: #BF0F00;';
    var lightRedItemStyle  = 'background-color: #F48B8B; color: #1a1a1a; border-color: #F44444;';
    var blueItemStyle      = 'background-color: #3e92cc; color: #fff; border-color: #3e92cc;';
    var greenItemStyle     = 'background-color: #30A00E; color: #fff; border-color: #30A00E;';
    var lavenderItemStyle  = 'background-color: #7871aa; color: #fff; border-color: #7871aa;';
    var defaultItemStyle   = 'background-color: #d5ddf6; color: #1a1a1a; border-color: #97b0f8;';


    // Overlap calculation data structures.
    var aggregateItems = {
      'sirs'                    : [],
      'organ_dysfunction'       : [],
      'trews_organ_dysfunction' : [],
      'hypotension'             : [],
      'hypoperfusion'           : [],
      'Ordered'                 : [],
      'Completed'               : []
    };

    // Track active subgroups.
    var criteriaGroupIds = {
      'sirs'                    : [],
      'organ_dysfunction'       : [],
      'trews_organ_dysfunction' : [],
      'hypotension'             : [],
      'hypoperfusion'           : [],
    };

    var orderGroupIds = [];


    // Add a custom time bar for severe sepsis start.
    if ( severe_sepsis_start != null ) {
      if ( this.chartHasSepsisMarker ) {
        this.chart.setCustomTime(severe_sepsis_start, 'v_cms_severe_sepsis');

      } else {
        this.chart.addCustomTime(severe_sepsis_start, 'v_cms_severe_sepsis');
        this.chartHasSepsisMarker = true;
      }
    } else {
      if ( this.chartHasSepsisMarker ) {
        try {
          this.chart.removeCustomTime('v_cms_severe_sepsis');
        } catch(err) {
          appendToConsole('Failed to timeline marker for v_cms_severe_sepsis: ' + err.message);
        }
        this.chartHasSepsisMarker = false
      }
    }

    // Add criteria items.
    var criteria_min_duration = 5*60*1000;
    for (var cls_i in criteriaClasses) {
      var pcls = criteriaClasses[cls_i]['pcls'];
      var cls = criteriaClasses[cls_i]['cls'];
      for (var c in json[pcls][cls]['criteria']) {
        var cr = json[pcls][cls]['criteria'][c];
        if ( cr['is_met'] ) {

          var start = new Date(cr['measurement_time'] * 1000);
          var end = null;
          var content = null;
          if ( cls == 'trews_organ_dysfunction' || cls == 'hypotension' || cls == 'hypoperfusion' ) {
            end = now;
            // Ensure min duration.
            if ( end - start < criteria_min_duration ) { end = new Date(start.getTime() + criteria_min_duration); }

            if ( cls == 'trews_organ_dysfunction' ) {
              content = 'Window to trigger TREWS alert starting @ ' + strToTime(start, true, true);
            } else {
              content = 'Window to trigger septic shock starting @ ' + strToTime(start, true, true);
            }
          } else {
            end = new Date(start.getTime() + 6 * 3600 * 1000);
            content = '6hr window to trigger CMS severe sepsis starting @ ' + strToTime(start, true, true);
          }

          var g_name = cr['name'] == 'initial_lactate' ? 'hpf_initial_lactate' : cr['name']; // HACK: map group name for hypoperfusion initial lactate.
          var g = this.groups[g_name];

          var v = cr['name'] == 'respiratory_failure' ? cr['value'] : Number(cr['value']).toPrecision(3);

          items.add({
            id: cr['name'],
            group: g['id'],
            content: content,
            title: content,
            start: start,
            end: end,
            type: 'range'
          });

          aggregateItems[cls].push({start: start, end: end});

          // Show active criteria initially.
          if ( this.groupDataSet.get(g['id']) == null ) {
            criteriaGroupIds[cls].push(g['id']);
            this.groupDataSet.add($.extend({}, g, { visible: visibleIds.indexOf(g['id']) >= 0 }))
          }
        }
      }
    }

    // Add trews organ dysfunction intervals
    var scoreParentId = 'trews_sevsep_score';
    var scoreParentGId = this.groups[scoreParentId]['id']

    for (var c in json['trews_intervals']) {
      var intervals = json['trews_intervals'][c];
      for (var i in intervals['intervals']) {
        var iv = intervals['intervals'][i];
        if ( iv['value'] ) {
          var start = new Date(iv['ts_start']);
          var end = new Date(iv['ts_end']);

          if ( intervals['name'] == 'trews_subalert' ) {
            var content = 'High Risk';
            var title = 'High Risk of Severe Sepsis from ' + strToTime(start, true, false) + ' to ' + strToTime(end, true, false);;

            var crossing = {
              id: 'crossing_' + i,
              group: this.groups['trewscore']['id'],
              content: content,
              start: start,
              end: end,
              title: title,
              type: 'range',
              style: redItemStyle
            };

            var parentCrossing = $.extend({}, crossing, { id: scoreParentId + '_' + crossing.id, group: scoreParentGId });
            items.add(crossing);
            items.add(parentCrossing);
          }
          else {
            var content = 'Window to trigger TREWS alert starting @ ' + strToTime(start, true, true);
            var g = this.groups[intervals['name']];

            items.add({
              id: intervals['name'] + '_' + i,
              group: g['id'],
              content: content,
              title: content,
              start: start,
              end: end,
              type: 'range'
            });

            aggregateItems['trews_organ_dysfunction'].push({start: start, end: end});

            // Show active criteria initially.
            if ( this.groupDataSet.get(g['id']) == null ) {
              criteriaGroupIds['trews_organ_dysfunction'].push(g['id']);
              this.groupDataSet.add($.extend({}, g, { visible: visibleIds.indexOf(g['id']) >= 0 }))
            }
          }
        }
      }
    }

    // Add orders.
    var order_keys = [
      'blood_culture',
      'initial_lactate',
      'crystalloid_fluid',
      'antibiotics',
      'repeat_lactate',
      'vasopressors'
    ];

    for (var i in order_keys) {
      var k = order_keys[i];
      var k2 = k + '_order';
      var g = this.groups[k];
      var g_class = null;

      var obj_with_status = null;
      var deadline_exceeded = false;

      var t_action = null;
      var t_condition = k == 'vasopressors' ? septic_shock_start : severe_sepsis_start;
      var t_deadline = k == 'vasopressors' ? septic_shock_deadline :
                        (k == 'repeat_lactate' ? severe_sepsis_deadline6 : severe_sepsis_deadline3);

      var unneeded_repeat_lactate_order =
        k2 == 'repeat_lactate_order' && json[k2]['is_met'] && json[k2]['status'] == null;

      if ( unneeded_repeat_lactate_order ) {
        if ( t_deadline != null ) {
          g_class = 'vis_g_' + k + '_not_needed';
        }
      }
      else if ( json[k2]['status'] != null ) {

        t_action = new Date(json[k2]['time'] * 1000);

        // Care completed maintenance.
        try {
          obj_with_status = JSON.parse(json[k2]['status']);
          if ( !('status' in obj_with_status) ) {
            obj_with_status = json[k2];
          }
        } catch(e) {
          obj_with_status = json[k2];
        }

        var order_status_complete =
          (k2 == 'repeat_lactate_order' ?
                  (json[k2]['is_met'] || orderStatusCompleted(obj_with_status))
                : (json[k2]['is_met'] && orderStatusCompleted(obj_with_status)));

        var order_complete = order_status_complete && t_deadline != null && t_action <= t_deadline;

        if ( k != 'vasopressors' ) {
          severe_sepsis_completed['status'] = severe_sepsis_completed['status'] && order_complete;
          severe_sepsis_completed['t'] = Math.max(severe_sepsis_completed['t'], t_action)
        }

        septic_shock_completed['status'] = septic_shock_completed['status'] && order_complete;
        septic_shock_completed['t'] = Math.max(septic_shock_completed['t'], t_action)

        // Tooltip.
        var tipPrefix = obj_with_status['status'] + ' at ' + strToTime(t_action, true, true);
        var tipPreDeadline = 'Need to complete by ' + strToTime(t_deadline, true, true);
        var tipPostDeadline = 'Deadline passed at ' + strToTime(t_deadline, true, true);

        var tooltip = tipPrefix;

        if ( obj_with_status['status'] == 'Ordered' && t_deadline != null && t_action <= t_deadline ) {
          tooltip = tipPrefix +  ' but not completed. ' +  tipPreDeadline;
        }
        else if ( obj_with_status['status'] == 'Ordered' && t_deadline != null && t_action > t_deadline ) {
          tooltip = 'Order must be completed. ' + tipPostDeadline;
        }
        else if ( t_deadline != null && t_action > t_deadline ) {
          tooltip = tipPrefix + '. ' + tipPostDeadline;
        }

        var itemBase = {
          id: json[k2]['name'],
          group: g['id'],
          content: '&nbsp;',
          title: tooltip,
          type: 'range',
          style: greenItemStyle
        };

        if ( !(k2 == 'repeat_lactate_order' ?
                    (json[k2]['is_met'] || orderStatusCompleted(json[k2]))
                  : (json[k2]['is_met'] && orderStatusCompleted(json[k2]))) ) {
          itemBase = $.extend({}, itemBase, { className: 'vis_item_order_incomplete' });
        }

        if ( t_condition != null ) {
          g_class = obj_with_status['status'] == 'Ordered' ? 'vis_g_' + k + '_incomplete' : null;

          // Action range.
          if ( t_action < t_condition ) {
            // Order status before onset.
            items.add($.extend({}, itemBase, {
              start: t_action,
              end: new Date(t_action.getTime() + range_as_point_duration)
            }));
          } else {
            // Order status after onset.
            items.add($.extend({}, itemBase, {
              start: t_condition,
              end: Math.min(Math.max(t_condition.getTime() + range_as_point_duration, t_action), t_deadline)
            }));
          }

          if ( t_action > t_deadline ) {
            // Warning range.
            // Order status after deadline.
            deadline_exceeded = true;

            items.add({
              id: json[k2]['name'] + '_warning',
              group: g['id'],
              content: '&nbsp;',
              title: tooltip,
              start: t_deadline,
              end: t_action,
              type: 'range',
              style: redItemStyle
            });
          }
        }
        else {
          // No onset.
          items.add($.extend({}, itemBase, {
            start: t_action,
            end: new Date(t_action.getTime() + range_as_point_duration),
          }));
        }

        aggregateItems[obj_with_status['status'] == 'Ordered' ? 'Ordered' : 'Completed'].push({
          t_action: t_action,
          t_condition: t_condition,
          t_deadline: t_deadline
        });
      }
      else {
        if ( t_deadline != null ) {
          // Not yet ordered.
          g_class = 'vis_g_' + k + '_incomplete';
        }

        // Mark bundles as incomplete.
        if ( k != 'vasopressors' ) { severe_sepsis_completed['status'] = false; }
        septic_shock_completed['status'] = false;
      }

      if ( !deadline_exceeded && t_deadline != null ) {

        if ( unneeded_repeat_lactate_order ) {
          tooltip = 'Order not needed';
        } else {
          // Deadline range.
          var tipPreDeadline = 'Need to complete by ' + strToTime(t_deadline, true, true);
          var tipPostDeadline = 'Deadline passed at ' + strToTime(t_deadline, true, true);

          var t_compare = t_action;
          if ( t_compare == null ) { t_compare = new Date(); }

          var tooltip = t_compare <= t_deadline ? tipPreDeadline : tipPostDeadline;
          if ( t_action != null && obj_with_status != null && obj_with_status['status'] == 'Completed' && t_action <= t_deadline ) {
            tooltip = 'Order completed before deadline passed at ' + strToTime(t_deadline, true, true);
          }
        }

        items.add({
          id: json[k2]['name'] + '_deadline',
          group: g['id'],
          content: '&nbsp;',
          title: tooltip,
          start: t_deadline,
          end: new Date(t_deadline.getTime() + range_as_point_duration),
          type: 'range',
          style: unneeded_repeat_lactate_order ? midGreyItemStyle : redItemStyle
        });
      }

      // Always include leaf order groups, and show them initially.
      if ( this.groupDataSet.get(g['id']) == null ) {
        orderGroupIds.push(g['id']);
        var extension = { visible: !this.chartInitialized || visibleIds.indexOf(g['id']) >= 0 };
        if ( g_class != null ) { extension['className'] = g_class; }
        this.groupDataSet.add($.extend({}, g, extension))
      }
    }

    // Severe sepsis and septic shock end times.
    severe_sepsis_completed['t'] = severe_sepsis_completed['status'] ? severe_sepsis_completed['t'] : null;
    septic_shock_completed['t'] = septic_shock_completed['status'] ? septic_shock_completed['t'] : null;

    var severe_sepsis_end = severe_sepsis_start == null ? null : now;
    severe_sepsis_end = severe_sepsis_reset == null ? severe_sepsis_end : Math.min(severe_sepsis_end, severe_sepsis_reset);
    severe_sepsis_end = severe_sepsis_completed['t'] == null ? severe_sepsis_end : Math.min(severe_sepsis_end, severe_sepsis_completed['t']);

    var septic_shock_end = septic_shock_start == null ? null : now;
    septic_shock_end = septic_shock_reset == null ? septic_shock_end : Math.min(septic_shock_end, septic_shock_reset);
    septic_shock_end = septic_shock_completed['t'] == null ? septic_shock_end : Math.min(septic_shock_end, septic_shock_completed['t']);



    // Compute aggregate groups.

    var hypotension_parents = [];
    var hypoperfusion_parents = [];

    if ( !severe_sepsis_by_cms && !severe_sepsis_by_trews ) {
      hypotension_parents = ['cms_hypotension', 'trews_sepshk_hypotension'];
      hypoperfusion_parents = ['cms_hypoperfusion', 'trews_sepshk_hypoperfusion'];
    } else {
      hypotension_parents = (severe_sepsis_by_cms ? ['cms_hypotension'] : []).concat(severe_sepsis_by_trews ? ['trews_sepshk_hypotension'] : []);
      hypoperfusion_parents = (severe_sepsis_by_cms ? ['cms_hypoperfusion'] : []).concat(severe_sepsis_by_trews ? ['trews_sepshk_hypoperfusion'] : []);
    }

    var segments = {
      sirs: {
        data: this.pairwiseOverlapUnion('sirs', aggregateItems, false, this.intersectOverlap),
        group_id: 'sirs',
        group_ctn: 'SIRS',
        parents: ['cms_sirs'],
        sty: lightRedItemStyle
      },
      organ_dysfunction: {
        data: this.pairwiseOverlapUnion('organ_dysfunction', aggregateItems, true, this.unionOverlap),
        group_id: 'org',
        group_ctn: 'CMS Organ DF',
        parents: ['cms_org'],
        sty: lightRedItemStyle
      },
      trews_organ_dysfunction: {
        data: this.pairwiseOverlapUnion('trews_organ_dysfunction', aggregateItems, true, this.unionOverlap),
        group_id: 'trews_org',
        group_ctn: 'TREWS Organ DF',
        parents: ['trews_sevsep_org'],
        sty: lightRedItemStyle
      },
      hypotension: {
        data: this.pairwiseOverlapUnion('hypotension', aggregateItems, true, this.unionOverlap),
        group_id: 'tension',
        group_ctn: 'Hypotension',
        parents: hypotension_parents,
        sty: lightRedItemStyle
      },
      hypoperfusion: {
        data: this.pairwiseOverlapUnion('hypoperfusion', aggregateItems, true, this.unionOverlap),
        group_id: 'fusion',
        group_ctn: 'Hypoperfusion',
        parents: hypoperfusion_parents,
        sty: lightRedItemStyle
      }
    };

    var segmentIndexes = $.map(criteriaClasses, function(i) { return i['cls']; });
    segmentIndexes.push('trews_organ_dysfunction');


    this.clsSegments = segments;

    // Create items for aggregated groups.
    var segmentIdsByGroup = {};

    for (var cls_i in segmentIndexes) {
      var cls = segmentIndexes[cls_i];
      var gId = segments[cls]['group_id'];
      var gCtn = segments[cls]['group_ctn'];
      var pgIds = segments[cls]['parents'];

      if ( segmentIdsByGroup[gId] == null ) {
        segmentIdsByGroup[gId] = 0;
      }

      var skipTime = segments[cls]['skip_ctn_time'] != null && segments[cls]['skip_ctn_time'];

      for (var i = 0; i < segments[cls]['data'].length; i++) {
        var itemBase = {
          content: gCtn + (skipTime ? '' : ' @ ' + strToTime(segments[cls]['data'][i].start, true, true)),
          title: gCtn + (skipTime ? '' : ' @ ' + strToTime(segments[cls]['data'][i].start, true, true)),
          start: segments[cls]['data'][i].start,
        };

        if ( segments[cls]['data'][i].end != null ) {
          itemBase = $.extend({}, itemBase, {
            end: segments[cls]['data'][i].end,
            type: 'range',
          });
        }
        else {
          itemBase = $.extend({}, itemBase, { type: 'box', });
        }

        if ( segments[cls]['group_title'] != null ) {
          itemBase = $.extend({}, itemBase, { title: segments[cls]['group_title'] });
        }

        if ( segments[cls]['subgroup_id'] != null ) {
          itemBase = $.extend({}, itemBase, { subgroup: segments[cls]['subgroup_id'] });
        }

        items.add($.extend({}, itemBase, {
          id: gId + '_' + (segmentIdsByGroup[gId] + i).toString(),
          group: this.groups[gId]['id'],
          style: segments[cls]['sty'] != null ? segments[cls]['sty'] : defaultItemStyle
        }));

        // Duplicate to parents.
        if ( pgIds != null ) {
          for (var j = 0; j < pgIds.length; j++) {
            var pgId = pgIds[j];
            var item = $.extend({}, itemBase, {
              id: pgId + '_' + gId + '_' + (segmentIdsByGroup[gId] + i).toString(),
              group: this.groups[pgId]['id']
            });

            if ( segments[cls]['psty'] != null ) {
              item = $.extend({}, item, { style: segments[cls]['psty'] });
            }

            items.add(item);
          }
        }
      }

      segmentIdsByGroup[gId] += segments[cls]['data'].length;
    }

    /*
    // Add score threshold crossings.
    var scoreValues = json['chart_data']['chart_values']['trewscore'];
    var scoreTsps = json['chart_data']['chart_values']['timestamp'];
    var threshold = json['chart_data']["trewscore_threshold"];

    var scoreParentId = 'trews_sevsep_score';
    var scoreParentGId = this.groups[scoreParentId]['id']

    var scoreSegmentsByGroup = {};
    scoreSegmentsByGroup[this.groups['trewscore']['id']] = [];
    scoreSegmentsByGroup[scoreParentGId] = [];

    var maxInSegment = null;
    var prevData = null;
    var prevCrossing = null;

    var numEntries = Math.max(scoreValues.length, scoreTsps.length);
    if ( numEntries > 0 && scoreValues[0] > threshold ) {
      prevCrossing = {v: scoreValues[0], t: scoreTsps[0]*1000};
    }

    var mkCrossingItem = function(groups, i, st, en, maxInSegment, style) {
      var shortRange = en - st <= (45 * 60 * 1000);

      var msg = shortRange ? '&nbsp;' : 'High Risk';
      // if (!shortRange && maxInSegment != null) { msg += ' (max ' + maxInSegment.toFixed(2) + ')'; }

      var title = 'High Risk of Severe Sepsis'
      // if (maxInSegment != null) { title += ' (max ' + maxInSegment.toFixed(2) + ')'; }
      title += ' from ' + strToTime(st, true, false) + ' to ' + strToTime(en, true, false);

      return {
        id: 'crossing_' + i,
        group: groups['trewscore']['id'],
        content: msg,
        start: new Date(st),
        end: new Date(en),
        title: title,
        type: 'range',
        style: style
      };
    }

    try {
      for (var i = 0; i < numEntries; i++) {
        var v = scoreValues[i];
        var t = scoreTsps[i]*1000;
        if ( prevData != null ) {
          var dropBelow = prevData.v >= threshold && v < threshold;
          var riseAbove = prevData.v <= threshold && v > threshold;

          if ( dropBelow ) {
            var crossing = mkCrossingItem(this.groups, i, prevCrossing.t, t, maxInSegment, redItemStyle);
            var parentCrossing = $.extend({}, crossing, { id: scoreParentId + '_' + crossing.id, group: scoreParentGId });
            scoreSegmentsByGroup[crossing['group']].push(crossing);
            scoreSegmentsByGroup[parentCrossing['group']].push(parentCrossing);
            maxInSegment = null;
            prevCrossing = null
          }
          else if ( riseAbove ) {
            // TODO: linear interpolation between prev and current?
            prevCrossing = {v: v, t: t};
            maxInSegment = v;
          }
        }
        prevData = {v: v, t: t};
        if ( maxInSegment != null ) { maxInSegment = Math.max(maxInSegment, v); }
      }

      // Final segment.
      if ( prevCrossing != null && numEntries > 0
            && prevCrossing.v > threshold && scoreValues[numEntries - 1] > threshold )
      {
        var crossing = mkCrossingItem(this.groups, numEntries - 1, prevCrossing.t, now.getTime(), maxInSegment, redItemStyle);
        var parentCrossing = $.extend({}, crossing, { id: scoreParentId + '_' + crossing.id, group: scoreParentGId });
        scoreSegmentsByGroup[crossing['group']].push(crossing);
        scoreSegmentsByGroup[parentCrossing['group']].push(parentCrossing);
      }

      // Append only the last segment for now, to be comparable to CMS.
      // This is because we only use the latest criteria in the REST API, rather than the history
      // of snapshots from the criteria_events table.
      // Note the above loop assumes a timestamp sorted order for trewscores, thus we do not need to re-sort.
      //
      // TODO: should we also check that a TREWS Org DF interval is present?
      // - Because update the criteria table on every TREWS edge, we only show intervals for the last
      //   edge to the current point in time.
      // - If the last edge corresponds to a False trews subalert, we will not have any orgdf interval.
      //
      if ( scoreSegmentsByGroup[this.groups['trewscore']['id']].length > 0 && scoreSegmentsByGroup[scoreParentGId].length > 0 ) {
        items.add(scoreSegmentsByGroup[this.groups['trewscore']['id']][scoreSegmentsByGroup[this.groups['trewscore']['id']].length - 1]);
        items.add(scoreSegmentsByGroup[scoreParentGId][scoreSegmentsByGroup[scoreParentGId].length - 1]);
      }
    } catch(e) {
      appendToConsole('Exception while calculating TREWS timeline intervals: ' + e.message);
    }
    */

    // Add point items.
    var severe_sepsis_treatment_lbl =
      'Treatment ' + (severe_sepsis_completed['t'] == null ?
        'incomplete' : 'completed at ' + strToTime(severe_sepsis_completed['t'], true, false));

    var septic_shock_treatment_lbl =
      'Treatment ' + (septic_shock_completed['t'] == null ?
        'incomplete' : 'completed at ' + strToTime(septic_shock_completed['t'], true, false));

    // We use separate onset times for TREWS/CMS for the respective severe sepsis groups,
    // but a shared ending time based on min(now, reset_time, completed_time).
    var sepsis_events = [
      { grp:  'suspicion_of_infection',
        pgrp: ['cms_soi', 'trews_sevsep_soi'],
        ob:   json['severe_sepsis'],
        ev:   'suspicion_of_infection',
        t:    'update_time',
        n:    'Note',
        tip:  'Suspected Source of Infection entered by ' +
                (json['severe_sepsis']['suspicion_of_infection'] != null ?
                  json['severe_sepsis']['suspicion_of_infection']['update_user'] : 'user'),
        end:  severe_sepsis_end,
        sty:  lightRedItemStyle,
        psty: defaultItemStyle,
        min_duration: 5*60*1000
      },
      { grp: 'cms_severe_sepsis',
        pgrp: null,
        ob:   json,
        ev:   'severe_sepsis',
        t:    'cms_onset_time',
        n:    'CMS Severe Sepsis onset',
        end:  severe_sepsis_end,
        sty:  redItemStyle,
        extend_now: true,
        extend_lbl: severe_sepsis_treatment_lbl
      },
      { grp: 'cms_septic_shock',
        pgrp: null,
        ob:   json,
        ev:   'septic_shock',
        t:    'onset_time',
        n:    'CMS Septic Shock onset',
        end:  septic_shock_end,
        sty:  redItemStyle,
        extend_now: true,
        extend_lbl: septic_shock_treatment_lbl
      },
      { grp: 'trews_severe_sepsis',
        pgrp: null,
        ob:   json,
        ev:   'severe_sepsis',
        t:    'trews_onset_time',
        n:    'TREWS Severe Sepsis onset',
        end:  severe_sepsis_end,
        sty:  redItemStyle,
        extend_now: true,
        extend_lbl: severe_sepsis_treatment_lbl
      },
      { grp: 'trews_septic_shock',
        pgrp: null,
        ob:   json,
        ev:   'septic_shock',
        t:    'onset_time',
        n:    'TREWS Septic Shock onset',
        end:  septic_shock_end,
        sty:  redItemStyle,
        extend_now: true,
        extend_lbl: septic_shock_treatment_lbl
      }
    ];

    for ( var i in sepsis_events ) {
      var obj = sepsis_events[i]['ob'];
      var evt = sepsis_events[i]['ev'];
      var tsp_field = sepsis_events[i]['t'];

      var lbl = null;
      var tip = null;
      var tsp = new Date(obj[evt][tsp_field]*1000);
      var g = this.groups[sepsis_events[i]['grp']];

      var itemId = sepsis_events[i]['grp'];

      var min_duration = sepsis_events[i]['min_duration'];

      if ( evt == 'suspicion_of_infection' && obj[evt][tsp_field] != null && obj[evt]['value'] != null ) {
        lbl = sepsis_events[i]['n'] + '(' + obj[evt]['value'] + ')';
        tip = sepsis_events[i]['tip'];
      }
      else if ( obj[evt]['is_met'] && obj[evt][tsp_field] != null ) {
        lbl = sepsis_events[i]['n'];
        tip = sepsis_events[i]['n'];
      }

      if ( lbl != null ) {
        var itemBase =  {
          content: lbl + ' @ ' + strToTime(tsp, true, true),
          title: tip + ' at ' + strToTime(tsp, true, true),
          start: tsp,
        };

        if ( sepsis_events[i]['end'] != null ) {
          // Ensure min duration.
          if ( min_duration != null && sepsis_events[i]['end'] - tsp < min_duration ) {
            itemBase = $.extend({}, itemBase, { end: new Date(tsp.getTime() + min_duration), type: 'range', });
          } else {
            itemBase = $.extend({}, itemBase, { end: sepsis_events[i]['end'], type: 'range', });
          }
        } else {
          itemBase = $.extend({}, itemBase, { type: 'box', });
        }

        if ( sepsis_events[i]['sty'] != null ) {
          itemBase = $.extend({}, itemBase, { style: sepsis_events[i]['sty'] });
        }

        items.add($.extend({}, itemBase, {
          id: itemId,
          group: g['id'],
          style: sepsis_events[i]['sty']
        }));

        // Add a second bar extending to present time.
        if ( sepsis_events[i]['extend_now'] != null && sepsis_events[i]['extend_now']
              && sepsis_events[i]['extend_lbl'] != null
              && sepsis_events[i]['end'] != null && sepsis_events[i]['end'] < now )
        {
          items.add({
            id: itemId + '_to_now',
            group: g['id'],
            content: '&nbsp;',
            title: lbl + ' at ' + strToTime(tsp, true, true) + '. ' + sepsis_events[i]['extend_lbl'] ,
            start: sepsis_events[i]['end'],
            end: now,
            style: greyItemStyle
          });
        }

        // Add to parent group if available.
        if ( sepsis_events[i]['pgrp'] != null ) {
          for (var j = 0; j < sepsis_events[i]['pgrp'].length; j++) {
            var pg = this.groups[sepsis_events[i]['pgrp'][j]];
            var item = $.extend({}, itemBase, {
              id: sepsis_events[i]['pgrp'][j] + '_' + itemId,
              group: pg['id']
            });

            if ( sepsis_events[i]['psty'] != null ) {
              item = $.extend({}, item, { style: sepsis_events[i]['psty'] });
            }

            items.add(item);
          }
        }
      }
    }

    // Create additional chart data structures.
    this.groupDataSet.add([
      { id           : this.groups['trews_org']['id'],
        content      : "TREWS Organ Dysfunction",
        nestedGroups : criteriaGroupIds['trews_organ_dysfunction'],
        showNested   : expandedParents.indexOf(this.groups['trews_org']['id']) >= 0
      },
      { id           : this.groups['sirs']['id'],
        className    : 'vis_g_cms_sirs',
        content      : "CMS SIRS",
        nestedGroups : criteriaGroupIds['sirs'],
        showNested   : expandedParents.indexOf(this.groups['sirs']['id']) >= 0
      },
      { id           : this.groups['org']['id'],
        content      : "CMS Organ Dysfunction",
        nestedGroups : criteriaGroupIds['organ_dysfunction'],
        showNested   : expandedParents.indexOf(this.groups['org']['id']) >= 0
      },
      { id           : this.groups['tension']['id'],
        className    : 'vis_g_hypotension',
        content      : "Hypotension",
        nestedGroups : criteriaGroupIds['hypotension'],
        showNested   : expandedParents.indexOf(this.groups['tension']['id']) >= 0
      },
      { id           : this.groups['fusion']['id'],
        content      : "Hypoperfusion",
        nestedGroups : criteriaGroupIds['hypoperfusion'],
        showNested   : expandedParents.indexOf(this.groups['fusion']['id']) >= 0
      },
      { id           : this.groups['orders']['id'],
        className    : 'vis_g_orders',
        content      : "Orders",
        nestedGroups : orderGroupIds,
        showNested   : !this.chartInitialized || expandedParents.indexOf(this.groups['orders']['id']) >= 0
      }
    ]);

    // this.chart.setOptions(this.chartOptions);
    this.chart.setGroups(this.groupDataSet);
    this.chart.setItems(items);

    if ( !this.chartInitialized ) {
      this.zoomToFit();
      this.chartInitialized = true;
    }

    if ( this.timelineVisible ) {
      this.chartHdr.removeClass('hidden').addClass('unhidden');
      this.chartCtn.removeClass('hidden').addClass('unhidden');
      this.zoomCtn.removeClass('hidden').addClass('unhidden');
      this.expandBtn.text('Hide Clinical Timeline');
    } else {
      this.chartHdr.removeClass('unhidden').addClass('hidden');
      this.chartCtn.removeClass('unhidden').addClass('hidden');
      this.zoomCtn.removeClass('unhidden').addClass('hidden');
      this.expandBtn.text('Show Clinical Timeline');
    }

    this.expandBtn.unbind();
    this.expandBtn.click(function(e) {
      if ( timeline.chartCtn.hasClass('hidden') ) {
        timeline.chartHdr.removeClass('hidden').addClass('unhidden');
        timeline.chartCtn.removeClass('hidden').addClass('unhidden');
        timeline.zoomCtn.removeClass('hidden').addClass('unhidden');
      } else {
        timeline.chartHdr.removeClass('unhidden').addClass('hidden');
        timeline.chartCtn.removeClass('unhidden').addClass('hidden');
        timeline.zoomCtn.removeClass('unhidden').addClass('hidden');
      }
      timeline.timelineVisible = !timeline.timelineVisible;
      timeline.expandBtn.text(timeline.timelineVisible ? 'Hide Clinical Timeline' : 'Show Clinical Timeline');
    });
  }
 }

// Utilities
/**
 * outputs a text friendly output of time elapsed
 * @param Date Object, input time
 * @return {String} formatted time lapsed
 */
function timeLapsed(d, time_first) {
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
    return time_first ?
        "at " + d.toLocaleTimeString() + " on " + d.toLocaleDateString()
      : "on " + d.toLocaleDateString() + " at " + d.toLocaleTimeString();
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
