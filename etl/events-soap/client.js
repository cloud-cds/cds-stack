var express = require('express');
var soap = require('soap');
var bodyParser = require('body-parser');
require('body-parser-xml')(bodyParser);
var app = express();
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
})
app.get('/test',function(req,res){
    var url = "https://event-tst.jh.opsdx.io/?wsdl";
    // var args = {eventInfo: {Type: {$value: "Test"}}};
    var args = {
    "eventInfo": {
        "attributes": {
            "xsi:type": "s01:NotificationEvent"
        },
    }
    }
};

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
// app.post('/getAge',bodyParser.urlencoded({extended:false}),function(req,res){
//     console.log(req.body);
//     var input = req.body;
//     console.log(input.biodata.weight);
//     console.log(input.biodata.height);
//     /*
//     -beginning of soap body
//     -url is defined to point to server.js so that soap cient can consume soap server's remote service
//     -args supplied to remote service method
//     */
//     var url = "http://localhost:3030/bmicalculator?wsdl";
//     var args = {weight:input.biodata.weight,height:input.biodata.height};
//     soap.createClient(url,function(err,client){
//         if(err)
//             console.error(err);
//         else {
//             client.calculateBMI(args,function(err,response){
//                 if(err)
//                     console.error(err);
//                 else{
//                     console.log(response);
//                     res.send(response);
//                 }
//             })
//         }
//     });
// })
var server = app.listen(3036,function(){
    var host = "127.0.0.1";
    var port = server.address().port;
    console.log("server running at http://%s:%s\n",host,port);
})