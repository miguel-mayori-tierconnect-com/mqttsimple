/**
 * Created by mike.mayori on 12/12/16.
 */
'use strict';

var mqtt = require('mqtt');
var _ = require('underscore');
var StringBuffer = require("sb-js").StringBuffer;

var itemFactoryMsg =  {
    sn: null,
    contractId:  null,
    timeStamp : null,
    contract : null,
    SKU: null,
    rfidTag : null
};
var contractFactoryMsg = {
    sn: null,
    contractId:  null,
    timeStamp : null,
    ASN_Carrier : null,
    PO_Date : null,
    PO_Customer : null,
    PO_Vendor : null,
    BOL_Consignee : null,
    ASN_DeliveryDate:null,
    BOL_Number:null,
    BOL_Date:null,
    Truck_ID:null,
    ASN_Number:null,
    ASN_ShippingDate:null,
    BOL_Shipper:null,
    Pallet_ID : null,
    Container_ID:null,
    status :  null,
    errorMessage : null
};
var clients = mqtt.connect({ port: 1883, host: 'hackaton-iot.vizix.io', keepalive: 10000});
clients.on('connect', function() {
    clients.subscribe('/v1/data/ALEB' );
    console.log('Connected..');
});
clients.on('message', function(topic, message)  {
    var result = parseMessageOn(topic, message);
    console.log(topic);
    console.log(message);

});

function parseMessageOn(topic, message) {
    var result = {};
    var sbuf = new StringBuffer();
    sbuf.add(message);
    var str = sbuf.as_lines();
    if(topic === '/v1/data/ALEB') {
        console.log(str);
        // var mqttMsg = getJsonFromStream(str, contractFactoryMsg);
    }
    result = str;//cleanMsg(mqttMsg);
    return result;
}

function cleanMsg(mqttMsg) {
    var newObject = {};
    _.each(mqttMsg, function(v, k) {
        if(v !== null && v !== undefined) {
            newObject[k] = v
        }
    });
    return newObject;
}

function getJsonFromStream(msg, facMsg) {
    _.each(msg, function(lineMsg){
        var lineSplitted = lineMsg.split(',');
        if (lineSplitted.length==2) {
            facMsg.sn = lineSplitted[1];
        } else {
            if (lineSplitted.length>2) {
                facMsg[lineSplitted[2]] = lineSplitted[3];
                if (_.isNull(facMsg.contractId)){
                    facMsg.contractId = lineSplitted[0];
                    facMsg.timeStamp = parseInt(lineSplitted[1]);
                }
            }
        }
    });
    return facMsg;
}