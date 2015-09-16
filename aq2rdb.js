'use strict';
var http = require('http');
var httpdispatcher = require('httpdispatcher');
var querystring = require('querystring');
var syncRequest = require('sync-request'); // make synchronous HTTP requests

var PORT = 8081; 

function handleRequest(request, response){
    try {
        console.log(request.url);
        httpdispatcher.dispatch(request, response);
    } catch (err) {
        console.log(err);
    }
}

// HTTP query parameter existence and non-empty content validation
function getParameter(parameter, response) {
    if (parameter === undefined) {
	// "Bad Request" HTTP status code
	var statusMessage = 'Required parameter not present';
	response.writeHead(400, statusMessage,
			   {'Content-Length': statusMessage.length,
			    'Content-Type': 'text/html'});
	response.end();
    }
    else if (parameter.trim() == '') {
	// "Bad Request" HTTP status code
	var statusMessage = 'No content in parameter';
	response.writeHead(400, statusMessage,
			   {'Content-Length': statusMessage.length,
			    'Content-Type': 'text/html'});
	response.end();
    }
    return parameter;
}

// GET request handler
httpdispatcher.onGet('/aq2rdb', function(request, response) {
    // parse HTTP query parameters in GET request URL
    var arg = querystring.parse(request.url);
    var username = getParameter(arg.Username, response);
    var password = getParameter(arg.Password, response);
    var z = getParameter(arg.z, response);
    var t = getParameter(arg.t, response);

    // data type ("t") parameter domain validation
    var msg;
    switch (t) {
    case 'ms':
        msg = 'Pseudo-time series (e.g., gage inspections) are not supported';
        break;
    case 'vt':
	msg = 'Sensor inspections and readings are not supported';
        break;
    case 'pk':
	msg = 'Peak-flow data are not supported';
	break;
    case 'dc':
	msg = 'Data corrections are not supported';
	break;
    case 'sv':
	msg = 'Quantitative site-visit data are not supported';
	break;
    case 'wl':
	msg = 'Discrete groundwater-levels data are not supported';
	break;
    case 'qw':
	msg = 'Discrete water quality data are not supported';
	break;
    default:
	msg = 'Unknown \"t\" parameter value: \"' + t + '\"';
    }

    console.log('Username: ' + username);
    console.log('Password: ' + password);
    console.log('z: ' + z);
    console.log('t: ' + t);

    // send (synchronous) request to GetAQToken service for AQUARIUS
    // authentication token
    var getAQTokenResponse =
        syncRequest(
            'GET',
            'http://localhost:8080/services/GetAQToken?&userName=' +
		username + '&password=' + password +
		'&uriString=http://nwists.usgs.gov/AQUARIUS/'
        );
    
    // TODO: need to handle AQUARIUS server GET response errors before
    // here

    // var token = getAQTokenResponse.getBody().toString('utf-8');

    // TODO: AQUARIUS Web service request and callback goes here

    // TODO: move to callback
    response.writeHead(200, {'Content-Type': 'text/plain'});       
    // TODO: RDB output goes here
    response.end('aq2rdb');
});    

var server = http.createServer(handleRequest);

server.listen(PORT, function(){
    console.log('Server listening on: http://localhost:%s', PORT);
});
