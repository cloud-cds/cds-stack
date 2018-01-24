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
  region          : process.env.AWS_REGION,
  accessKeyId     : process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey : process.env.AWS_SECRET_ACCESS_KEY
});

var service = {
  EventService : {
    EventPort : {
      ProcessEvent: function(args){
        console.log('new event message:');
        console.log(JSON.stringify(args, null, 4));

        producer.send([{
          id: args['eventInfo']['Type']['$value'],
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
        myMetric.put(1, 'EventCount_'+ args['eventInfo']['Type']['$value'].replace('-','_').replace(' ',''));
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
