var soap = require('soap');
var express = require('express');
var app = express();
var cloudwatchMetrics = require('cloudwatch-metrics');
var moment = require('moment');

var myMetric = new cloudwatchMetrics.Metric('OpsDX', 'Count', [{
  Name: 'API',
  Value: process.env.metrics_dimension_value
}], {
  sendInterval: 1 * 1000, // It's specified in milliseconds.
  sendCallback: (err) => {
  if (!err) return;
  // Do your error handling here.
  console.log(err)
  }
});

if (!('toJSON' in Error.prototype))
Object.defineProperty(Error.prototype, 'toJSON', {
    value: function () {
        var alt = {};

        Object.getOwnPropertyNames(this).forEach(function (key) {
            alt[key] = this[key];
        }, this);

        return alt;
    },
    configurable: true,
    writable: true
});

var sqs = require('sqs-producer');

var producer = sqs.create({
  queueUrl        : process.env.queue_url,
  region          : process.env.AWS_DEFAULT_REGION,
  accessKeyId     : process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey : process.env.AWS_SECRET_ACCESS_KEY
});

/* Summary logging */
var type_summary = {};
var type_overflow = {};

var summary_periods_secs = parseInt(process.env.summary_periods_secs, 30);

function log_summary_loop() {
  for ( var ty in type_summary ) {
    var ext = ' (' + ((ty in type_overflow) ? type_overflow[ty] : 0) + ')';
    console.log(ty + ' => ' + type_summary[ty] + ext);
  }

  setTimeout(log_summary_loop, summary_periods_secs * 1000);
}

log_summary_loop();

var service = {
  EventService : {
    EventPort : {
      ProcessEvent: function(args){
        var has_event_info = 'eventInfo' in args;
        var has_ei_type = has_event_info ? 'Type' in args['eventInfo'] : false;
        var has_eit_value = has_ei_type ? '$value' in args['eventInfo']['Type'] : false;

        if ( !has_eit_value ) {
          myMetric.put(1, 'EventCount_TYPE_ERROR');
          return {};
        }

        var event_type = args['eventInfo']['Type']['$value'].replace('-','_').replace(' ','');

        if ( !(event_type in type_summary) ) { type_summary[event_type] = 0; }
        type_summary[event_type] += 1;

        if ( type_summary[event_type] < 0 ) {
          if ( !(event_type in type_overflow) ) { type_overflow[event_type] = 0; }
          type_overflow[event_type] += 1;
          type_summary[event_type] = 1;
        }

        producer.send([{
          id: 'id1',
          body: JSON.stringify(args)
        }], function(err) {
          if (err) {
            myMetric.put(1, 'EventCount_FW_ERROR');
            console.log(err);
          } else {
            myMetric.put(1, 'EventCount_FW_SUCCESS');
          }
        });

        myMetric.put(1, 'EventCount');
        myMetric.put(1, 'EventCount_'+ event_type);
        return {};
      }
    }
  }
};

// xml data is extracted from wsdl file created
var xml = require('fs').readFileSync('./EventNotification.wsdl','utf8');
var server = app.listen(8000,function(){
  var host = "127.0.0.1";
  var port = server.address().port;
});
soap.listen(server,'/',service,xml);
