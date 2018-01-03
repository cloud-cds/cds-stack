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
var count = 0;
var max_count = parseInt(process.env.max_count);
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
                var request = require('request');

                // TODO: enable event forwarding and use the right URL
                // var options = {
                //   uri: 'https://event-dev.jh.opsdx.io',
                //   method: 'POST',
                //   json: args
                // };

                // request(options, function (error, response, body) {
                //   if (!error && response.statusCode == 200) {
                //     console.log(body.id) // Print the shortened url.
                //   }
                // });
                count = count + 1;
                if(count >= max_count)
                {
                  myMetric.put(count, 'EventCount');
                  count = 0;
                }
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