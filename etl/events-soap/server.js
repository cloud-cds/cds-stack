var soap = require('soap');
var express = require('express');
var app = express();
/**
-this is remote service defined in this file, that can be accessed by clients, who will supply args
-response is returned to the calling client
-our service calculates bmi by dividing weight in kilograms by square of height in metres
**/
var service = {
    BMI_Service : {
        BMI_Port :{
            calculateBMI:function(args){
                //console.log(Date().getFullYear())
                var year = new Date().getFullYear();
                var n = (args.weight)/(args.height*args.height);
                console.log(n);
                return {bmi: n};
            }
        }
    }
};
// xml data is extracted from wsdl file created
var xml = require('fs').readFileSync('./bmicalculator.wsdl','utf8');
var server = app.listen(3030,function(){
    var host = "127.0.0.1";
    var port = server.address().port;
});
soap.listen(server,'/bmicalculator',service,xml);