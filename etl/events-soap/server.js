var soap = require('soap');
var express = require('express');
var app = express();
var cloudwatchMetrics = require('cloudwatch-metrics');
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
var event_forward = process.env.event_forward;
var request = require('request');
/**
-this is remote service defined in this file, that can be accessed by clients, who will supply args
-response is returned to the calling client
-our service calculates bmi by dividing weight in kilograms by square of height in metres
**/
var service = {
  EventService : {
    EventPort : {
      ProcessEvent:function(args){
        console.log('new event message:');
        console.log(JSON.stringify(args, null, 4));
        if(event_forward != ''){
          // enable event forwarding and use the right URL
          var options = {
            uri: event_forward,
            method: 'POST',
            json: args
          };

          request(options, function (error, response, body) {
            if (!error && response.statusCode == 200) {
            console.log("forwarded event type:", args['eventInfo']['Type']['$value']) // Print the shortened url.
            }
            else {
            console.log("error code:", response.statusCode);
            }
          });
        }
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