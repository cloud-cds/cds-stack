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
        "Type": {
            "attributes": {
                "xsi:type": "s:string"
            },
            "$value": "Flowsheet - Add"
        },
        "DisplayName": {
            "attributes": {
                "xsi:type": "s:string"
            },
            "$value": "Flowsheet - Add"
        },
        "TimeGenerated": {
            "attributes": {
                "xsi:type": "s:dateTime"
            },
            "$value": "2018-01-04T02:11:01.000Z"
        },
        "Status": {
            "attributes": {
                "xsi:type": "s:string"
            }
        },
        "OtherEntities": [
            {
                "Entity": {
                    "attributes": {
                        "xsi:type": "s01:NotificationEntity"
                    },
                    "Type": {
                        "attributes": {
                            "xsi:type": "s:string"
                        },
                        "$value": "FLO"
                    },
                    "ID": {
                        "attributes": {
                            "xsi:type": "s:string"
                        },
                        "$value": "5"
                    },
                    "IDType": {
                        "attributes": {
                            "xsi:type": "s:string"
                        },
                        "$value": "External"
                    },
                    "DisplayName": {
                        "attributes": {
                            "xsi:type": "s:string"
                        },
                        "$value": "BLOOD PRESSURE"
                    },
                    "ContactDate": {
                        "attributes": {
                            "xsi:type": "s:string"
                        }
                    }
                }
            }
        ],
        "User": {
            "attributes": {
                "xsi:type": "s01:NotificationEntity"
            },
            "Type": {
                "attributes": {
                    "xsi:type": "s:string"
                },
                "$value": "EMP"
            },
            "ID": {
                "attributes": {
                    "xsi:type": "s:string"
                },
                "$value": "EDRNHCGH"
            },
            "IDType": {
                "attributes": {
                    "xsi:type": "s:string"
                },
                "$value": "External"
            },
            "DisplayName": {
                "attributes": {
                    "xsi:type": "s:string"
                },
                "$value": "EMERGENCY, NURSE"
            }
        },
        "PrimaryEntity": {
            "attributes": {
                "xsi:type": "s01:NotificationEntity"
            },
            "Type": {
                "attributes": {
                    "xsi:type": "s:string"
                },
                "$value": "EPT"
            },
            "ID": {
                "attributes": {
                    "xsi:type": "s:string"
                },
                "$value": "Z6636863"
            },
            "IDType": {
                "attributes": {
                    "xsi:type": "s:string"
                },
                "$value": "External"
            },
            "DisplayName": {
                "attributes": {
                    "xsi:type": "s:string"
                },
                "$value": "TST,TREWSONE"
            },
            "ContactDate": {
                "attributes": {
                    "xsi:type": "s:string"
                },
                "$value": "11/22/2017"
            }
        }
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


