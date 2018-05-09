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
    limit:'1MB',
    xmlParseOptions:{
        normalize:true,
        normalizeTags:true,
        explicitArray:false
    }
}));
app.get('/',function(req,res){
    res.sendFile(__dirname + "/" + "/client.html");
});
app.get('/test',function(req,res){
    var url = "https://localhost:8000/?wsdl";
    var args = {eventInfo: {Type: {$value: "Test"}}};

    soap.createClient(url,function(err,client){
        if(err)
            console.error(err);
        else {
            console.log(client)
            console.log(args)
            client.ProcessEvent(args,function(err,response){
                if(err)
                    console.error(err);
                else{
                    console.log(response);
                    res.send(response);
                }
            })
        }
    });

    // soap.createClientAsync(url).then((client) => {
    //   return client.ProcessEvent(args);
    // }).then((result) => {
    //   console.log(result);
    // });
})
app.get('/local',function(req,res){
    var url = "http://localhost:8000/?wsdl";
    var args = {event:0};
    soap.createClient(url,function(err,client){
        if(err)
            console.error(err);
        else {
            console.log(client)
            client.ProcessEvent(args,function(err,response){
                if(err)
                    console.error(err);
                else{
                    console.log(response);
                    res.send(response);
                }
            })
        }
    });
})

app.get('/tst',function(req,res){
    var url = "https://event-tst.jh.opsdx.io/?wsdl";
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
                    res.send(response);
                }
            })
        }
    });
})

app.get('/poc',function(req,res){
    var url = "https://event-poc.jh.opsdx.io/?wsdl";
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
                    res.send(response);
                }
            })
        }
    });
})

var server = app.listen(3036,function(){
    var host = "127.0.0.1";
    var port = server.address().port;
    console.log("server running at http://%s:%s\n",host,port);
})