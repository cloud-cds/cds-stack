var express = require('express');
var soap = require('soap');
var bodyParser = require('body-parser');
require('body-parser-xml')(bodyParser);
var app = express();
var args_flo = {
    "eventInfo": {
        "attributes": {
            "xsi:type": "s01:NotificationEvent"
        },
    }
};

app.use(bodyParser.xml({
    limit:'10MB',
    xmlParseOptions:{
        normalize:true,
        normalizeTags:true,
        explicitArray:false
    }
}));

var seconds = 600;
function request(){
    var url = "https://event-poc.jh.opsdx.io/?wsdl";
    for(i = 0; i< 200; i++)
        soap.createClient(url,function(err,client){
            if(err)
                console.error(err);
            else {
                client.ProcessEvent(args_flo,function(err,response){
                    if(err)
                        console.error(err);
                    else{
                        console.log(response);
                        console.log('succeed!')
                    }
                })
            }
        });
    if(seconds == 0)
        return
    seconds -= 1
    setTimeout(request, 1000);
};

request();


