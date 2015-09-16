'use strict';
var http = require('http');
var httpdispatcher = require('httpdispatcher');
var querystring = require('querystring');

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

httpdispatcher.onGet('/aq2rdb', function(request, response) {
    response.writeHead(200, {'Content-Type': 'text/plain'});
        // parse HTTP query parameters in GET request URL
    var arg = querystring.parse(request.url);
    var z = getParameter(arg.z, response);
    var t = getParameter(arg.t, response);

    console.log('z: ' + z);
    console.log('t: ' + t);
    
    // send (synchronous) request to GetAQToken for AQUARIUS token
    /*
    var getAQTokenResponse =
            syncRequest(
                'GET',
                'http://localhost:8080/services/GetAQToken?&userName=apiuser&password=2KcbuMDkSHMl&uriString=http://nwists.usgs.gov/AQUARIUS/'
            );
    */
    
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
    console.log("Server listening on: http://localhost:%s", PORT);
});
