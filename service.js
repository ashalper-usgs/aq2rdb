/**
 * @fileOverview Prototypes for querying aq2rdb prerequisite Web
 *               services.
 *
 * @module service
 *
 * @author Andrew Halper <ashalper@usgs.gov>
 */

'use strict';

// Node.JS modules
var async = require("async");
var https = require('https');
var fs = require("fs");
var querystring = require("querystring");

var service = module.exports = {

/**
   @class
   @classdesc NWIS-RA system interface prototype.
   @public
   @param {string} host DNS host name of NWIS-RA instance.
   @param {string} userName NWIS-RA account, user name.
   @param {string} password NWIS-RA account password.
   @param {boolean} log Enable aq2rdb server logging if true; no
          logging if false.
   @param {function} callback Callback function to call when
          construction complete.
*/
NWISRA: function (host, userName, password, log, callback) {
    var authentication;
    var parameters;

    /**
       @method
       @description Get authentication token from NWIS-RA.
       @private
     */
    function authenticate(callback) {
        async.waterfall([
            function (callback) {
                var path = "/service/auth/authenticate";

                /**
                   @description Handle response from HTTPS query.
                   @callback
                */
                function queryCallback(response) {
                    var messageBody = "";

                    // accumulate response
                    response.on(
                        "data",
                        function (chunk) {
                            messageBody += chunk;
                        });

                    response.on("end", function () {
                        // "401 Unauthorized": authentication has failed
                        if (response.statusCode === 401) {
                            callback(
                                "Could not login to NWIS-RA Web services"
                            );
                            return;
                        }
                        callback(null, messageBody);
                    });
                }

                var request = https.request({
                    host: host,
                    method: "POST",
                    headers:
                        {"content-type": "application/x-www-form-urlencoded"},
                    path: path
                }, queryCallback);

                /**
                   @description Handle service invocation errors.
                */
                request.on("error", function (error) {
                    if (error.code === "ENOTFOUND") {
                        // can't get to the NWIS-RA server; use local file
                        fs.readFile("parameters.json", function (error, json) {
                            if (error) {
                                callback(error);
                                return;
                            }

                            try {
                                parameters = JSON.parse(json);
                            }
                            catch (error) {
                                callback(error);
                                return;
                            }
                        });
                        callback(null, null);
                    }
                    else
                        callback(error);
                    return;
                });

                // authentication credentials get POSTed to the server
                request.write(
                    querystring.stringify(
                        {username: userName, password: password}
                    )
                );

                request.end();  // end transaction
            },
            function (messageBody, callback) {
                if (messageBody) {
                    try {
                        authentication = JSON.parse(messageBody);
                    }
                    catch (error) {
                        callback(error);
                        return;
                    }
                }
                callback(null);
            }
        ],
            function (error) {
                if (error)
                    callback(error);
                else
                    callback(null);
            }
        );
    } // authenticate

    /**
       @method
       @description Query an NWIS-RA Web service.
       @private
       @param {string} path Path to Web service endpoint.
       @param {object} obj Web service REST parameters.
       @param {boolean} log Enable logging if true; no logging otherwise.
       @callback {function} Callback function to call when query complete.
     */
    function queryRemote(path, obj, log, callback) {
        /**
           @description Handle response from HTTPS query.
           @callback
        */
        function queryCallback(response) {
            var messageBody = '';

            // accumulate response
            response.on(
                'data',
                function (chunk) {
                    messageBody += chunk;
                });

            response.on('end', function () {
                if (
                    response.statusCode < 200 || 300 <= response.statuscode
                ) {
                    callback(
                        "Received an error from http://" + host +
                            path + "; HTTP status code was: " +
                            response.statusCode.toString()
                    );
                    return;
                }
                callback(null, messageBody);
                return;
            });
        } // queryCallback

        var request = https.request({
            host: host,
            method: "GET",
            headers: {"Authorization": "Bearer " + authentication.tokenId},
            path: path + '?' + querystring.stringify(obj)
        }, queryCallback);

        /**
           @description Handle service invocation errors.
        */
        request.on("error", function (error) {
            if (log)
                console.log("service.NWISRA.queryRemote: " + error);
            callback(error);
            return;
        });

        request.end();

    } // queryRemote

    /**
       @method
       @description Make an NWIS-RA, HTTP query.
       @public
       @param {object} obj HTTP query parameter/value object.
       @param {boolean} log Enable console logging if true; no console
                        logging when false.
       @param {function} callback Callback function to call if/when
                         response is received.
    */
    this.query = function (obj, log, callback) {
        // if NWIS-RA is logged-into
        if (authentication) {
            queryRemote(
                "/service/data/view/parameters/json",
                obj,
                log,
                callback
            );
        }
        else {
            // emulate the query by referring to local copy of JSON data
            var parameter = parameters.find(function (record) {
                if (obj["parameters.PARM_CD"] === record.PARM_CD)
                    return true;
                else
                    return false;
            });

            if (parameter === undefined) {
                callback(
                    "Could not find a corresponding AQUARIUS " +
                        "Parameter name for USGS parameter code \"" +
                        record.PARM_CD + "\""
                );
                return;
            }

            callback(parameter);
        }
    } // query

    // constructor
    var host = host;
    var log = log;

    authenticate(callback);

} // NWISRA

}
