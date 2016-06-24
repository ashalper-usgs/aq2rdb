/**
 * @fileOverview Prototypes to interface with Aquatic Informatics,
 *               Inc. Web services (e.g. AQUARIUS).
 *
 * @module service
 *
 * @author Andrew Halper <ashalper@usgs.gov>
 */

'use strict';

// Node.JS modules
var http = require("http");
var querystring = require("querystring");
var url = require("url");

// aq2rdb modules
var rest = require("./rest");

var aquaticInformatics = module.exports = {

/**
   @class
   @classdesc LocationIdentifier object prototype.
   @public
   @param {string} text AQUARIUS LocationIdentifier string.
*/
LocationIdentifier: function (
    /* agencyCode, siteNumber | LocationIdentifier (AQUARIUS type) */
) {
    var agencyCode, siteNumber;

    // LocationIdentifier constructor
    if (arguments.length == 1) {
        if (arguments[0].includes('-')) {
            var token = field.arguments[0].split('-');

            agencyCode = token[1];
            siteNumber = token[0];
        }
        else
            siteNumber = arguments[0];
    }
    // (agencyCode, siteNumber) constructor
    else if (arguments.length == 2) {
        var agencyCode = arguments[0];
        var siteNumber = arguments[1];
    }

    /**
       @method
       @description Agency code accessor method.
    */
    this.agencyCode = function () {
        if (agencyCode === undefined)
            return "USGS";
        else
            return agencyCode;
    }

    /**
       @method
       @description Site number accessor method.
    */
    this.siteNumber = function () {
        return siteNumber;
    }

    /**
       @method
       @description Return a string representation of
                    LocationIdentifier.
    */
    this.toString = function () {
        if (agencyCode === "USGS")
            return siteNumber;
        else
            return siteNumber + '-' + agencyCode;
    }

}, // LocationIdentifier

/**
   @class
   @classdesc AQUARIUS object prototype.
   @public
   @param {string} aquariusTokenHostname DNS host name of
                   aquarius-token server.
   @param {string} hostname DNS host name of AQUARIUS server.
   @param {string} userName AQUARIUS account user name.
   @param {string} password AQUARIUS account password.
   @param {function} callback Callback to call when object
          constructed.
*/
AQUARIUS: function (
    aquariusTokenHostname, hostname, userName, password, callback
) {
    var aquariusTokenHostname, token;
    var port = "8080";
    var path = "/services/GetAQToken?";
    var uriString = "http://" + hostname + "/AQUARIUS/";

    if (aquariusTokenHostname === undefined) {
        callback('Required field "aquariusTokenHostname" not found');
        return;
    }

    if (aquariusTokenHostname === '') {
        callback('Required field "aquariusTokenHostname" must have a value');
        return;
    }

    aquariusTokenHostname = aquariusTokenHostname;

    if (hostname === undefined) {
        callback('Required field "hostname" not found');
        return;
    }

    if (hostname === '') {
        callback('Required field "hostname" must have a value');
        return;
    }

    this.hostname = hostname;

    if (userName === undefined) {
        callback('Required field "userName" not found');
        return;
    }

    if (userName === '') {
        callback('Required field "userName" must have a value');
        return;
    }

    if (password === undefined) {
        callback('Required field "password" not found');
        return;
    }

    if (password === '') {
        callback('Required field "password" must have a value');
        return;
    }

    /**
       @function
       @description GetAQToken service response callback.
       @private
       @callback
    */
    function getAQTokenCallback(response) {
        var messageBody = '';

        // accumulate response
        response.on('data', function (chunk) {
            messageBody += chunk;
        });

        // Response complete; token received.
        response.on('end', function () {
            token = messageBody;
            callback(
                null,
                "Received AQUARIUS authentication token successfully"
            );
            return;
        });
    } // getAQTokenCallback

    // make sure to not reveal user-name/passwords in log
    path += querystring.stringify(
        {userName: userName, password: password,
         uriString: uriString}
    );

    /**
       @description GetAQToken service request for AQUARIUS
                    authentication token needed for AQUARIUS API.
    */
    var request = http.request({
        host: aquariusTokenHostname,
        port: port,             // TODO: make a CLI parameter?
        path: path
    }, getAQTokenCallback);

    /**
       @description Handle GetAQToken service invocation errors.
       @callback
    */
    request.on('error', function (error) {
        var statusMessage;

        if (error.code === "ECONNREFUSED") {
            var message =               
                "Could not connect to GetAQToken service at " +
                error.address;

            if (error.port !== undefined)
                message += " on port " + error.port.toString();

            message += " for AQUARIUS authentication token";
            callback(message);
        }
        else {
            callback(error);
        }
        return;
    });

    request.end();

    /**
       @method
       @description AQUARIUS authentication token accessor method.
     */
    this.token = function () {
        return token;
    }

    /**
       @method
       @description Call AQUARIUS GetLocationData Web service.
       @param {string} locationIdentifier AQUARIUS location identifier.
    */
    this.getLocationData = function (locationIdentifier) {
        return rest.query(
            "http", hostname, "GET", undefined,
            "/AQUARIUS/Publish/V2/GetLocationData?" +
                querystring.stringify(
                    {token: token, format: "json",
                     LocationIdentifier: locationIdentifier}
                )
        );
    } // getLocationData

    /**
       @method
       @description Call AQUARIUS GetTimeSeriesCorrectedData Web service.
       @param {object} parameters AQUARIUS
              GetTimeSeriesCorrectedData service HTTP parameters.
       @param {function} callback Callback to call if/when
              GetTimeSeriesCorrectedData service responds.
    */
    this.getTimeSeriesCorrectedData = function (fields) {
        // these fields span every GetTimeSeriesCorrectedData
        // call for our purposes, so they're not passed in
        fields["token"] = token;
        fields["format"] = "json";

        /**
           @todo replace with return rest.query()
        */
        return rest.query(
            "http", hostname, "GET", undefined,
            "/AQUARIUS/Publish/V2/GetTimeSeriesCorrectedData", fields,
            false
        );
    } // getTimeSeriesCorrectedData

    /**
       @method
       @description Parse AQUARIUS TimeSeriesDataServiceResponse
                    received from GetTimeSeriesCorrectedData service.
       @param {string} messageBody Message from AQUARIUS Web service.
       @param {function} callback Callback function to call when
                                  response is received.
    */
    this.parseTimeSeriesDataServiceResponse = function (
        messageBody, callback
    ) {
        var timeSeriesDataServiceResponse;

        try {
            timeSeriesDataServiceResponse = JSON.parse(messageBody);
        }
        catch (error) {
            callback(error);
            return;
        }

        callback(null, timeSeriesDataServiceResponse);
    } // parsetimeSeriesDataServiceResponse

    /**
       @method
       @public
       @description Cache remark codes.
    */
    this.getRemarkCodes = function () {
        // if remark codes have not been loaded yet
        if (this.remarkCodes === undefined) {
            return rest.query(
                "http", hostname, "GET", undefined, // HTTP headers
                "/AQUARIUS/Publish/V2/GetQualifierList/",
                {token: token, format: "json"}, false
            ).then((messageBody) => {
                var qualifierListServiceResponse;

                try {
                    qualifierListServiceResponse =
                        JSON.parse(messageBody);
                }
                catch (error) {
                    throw error;
                    return;
                }

                // if we didn't get the remark codes domain table
                if (qualifierListServiceResponse === undefined) {
                    throw new Error(
                        "Could not get remark codes from http://" +
                            hostname +
                            "/AQUARIUS/Publish/V2/GetQualifierList/"
                    );
                    return;
                }

                // store remark codes in object for faster access later
                this.remarkCodes = new Object();
                var qualifiers = qualifierListServiceResponse.Qualifiers;
                for (var i = 0, l = qualifiers.length; i < l; i++) {
                    this.remarkCodes[qualifiers[i].Identifier] =
                        qualifiers[i].Code;
                }
            }); // .then
        }
    } // getRemarkCodes

    /**
       @function
       @description Distill a set of time series descriptions into
                    (hopefully) one, to query for a set of time series
                    date/value pairs.
       @private
       @param {object} timeSeriesDescriptions An array of AQUARIUS
              TimeSeriesDescription objects.
       @param {object} locationIdentifier A LocationIdentifier object.
    */
    function distill(timeSeriesDescriptions, locationIdentifier) {
        switch (timeSeriesDescriptions.length) {
        case 0:
            throw 'No time series descriptions found for ' +
                'LocationIdentifier "' + locationIdentifier + '"';
            break;
        case 1:
            break;
        default:
            var primaryTimeSeriesDescriptions = new Array();

            for (var i = 0, l = timeSeriesDescriptions.length; i < l; i++) {
                var tuples = timeSeriesDescriptions[i].ExtendedAttributes;

                tuples.find(function (t) {
                    // if this is a primary TimeSeriesDescription
                    if (t.Name === "PRIMARY_FLAG" &&
                        t.Value === "Primary")
                        // save it
                        primaryTimeSeriesDescriptions.push(
                            timeSeriesDescriptions[i]
                        );
                });
            }

            if (1 < primaryTimeSeriesDescriptions.length) {
                // raise error
                /**
                   @todo this is a hack arising from a lack of
                         sophistication in rdb.comment() algorithm (it
                         can only create correct RDB comments from
                         single-line strings)
                */
                var error =
                    "More than one primary time series found for \"" +
                    locationIdentifier.toString() + "\":\n" +
                    "#\n";

                for (var i = 0,
                     l = primaryTimeSeriesDescriptions.length; i < l;
                     i++)
                    error += "#   " +
                    JSON.stringify(primaryTimeSeriesDescriptions[i]) +
                    "\n";

                throw error;
            }
        } // switch (timeSeriesDescriptions.length)

        return timeSeriesDescriptions[0];
    } // distill

    /**
       @method
       @description Query AQUARIUS GetTimeSeriesDescriptionList
                    service to get list of AQUARIUS, time series
                    UniqueIds related to aq2rdb, location and
                    parameter.
       @public
       @param {object} field An object having
                       GetTimeSeriesDescriptionList Web service
                       field/values as attribute/value pairs.
       @see http://nwists.usgs.gov/AQUARIUS/Publish/v2/json/metadata?op=TimeSeriesDescriptionServiceRequest
    */
    this.getTimeSeriesDescriptionList = function (field) {
        field["token"] = token;
        field["format"] = "json";

        return rest.query(
            "http",
            hostname,
            "GET",
            undefined,      // HTTP headers
            "/AQUARIUS/Publish/V2/GetTimeSeriesDescriptionList",
            field,
            false
        );
    } // getTimeSeriesDescriptionList

    /**
       @method
       @description Get a TimeSeriesDescription object from AQUARIUS.
       @public
       @param {string} agencyCode USGS agency code.
       @param {string} siteNumber USGS site number.
       @param {string} parameter AQUARIUS parameter.
       @param {string} computationIdentifier AQUARIUS computation identifier.
       @param {string} computationPeriodIdentifier AQUARIUS computation
                       period identifier.
    */
    this.getTimeSeriesDescription = function (
        agencyCode, siteNumber, parameter, computationIdentifier,
        computationPeriodIdentifier
    ) {
        var locationIdentifier =
            new aquaticInformatics.LocationIdentifier(
                agencyCode, siteNumber
            );
        var timeSeriesDescriptions = function (messageBody) {
            return new Promise(function (resolve, reject) {
                var timeSeriesDescriptionListServiceResponse;

                try {
                    timeSeriesDescriptionListServiceResponse =
                        JSON.parse(messageBody);
                }
                catch (error) {
                    reject(error);
                    return;
                }

                resolve(
                timeSeriesDescriptionListServiceResponse.TimeSeriesDescriptions
                );
            });
        };
        var timeSeriesDescription =
            function (timeSeriesDescriptions, locationIdentifier) {
                return new Promise(function (resolve, reject) {
                    var t;

                    try {
                        t = distill(
                            timeSeriesDescriptions, locationIdentifier
                        );
                    }
                    catch (error) {
                        reject(error);
                    }
                    resolve(t);
                });
            };
        
        return this.getTimeSeriesDescriptionList({
            LocationIdentifier: locationIdentifier.toString(),
            Parameter: parameter,
            ComputationIdentifier: computationIdentifier,
            ComputationPeriodIdentifier: computationPeriodIdentifier,
            ExtendedFilters: "[{FilterName:PRIMARY_FLAG,FilterValue:Primary}]"
        })
            .then((messageBody) => {
                return timeSeriesDescriptions(messageBody);
            })
            .then((timeSeriesDescriptions) => {
                return timeSeriesDescription(
                    timeSeriesDescriptions, locationIdentifier
                );
            })
            .catch((error) => {
                if (error === 400)
                    throw "No time series description list found at " +
                            url.format({
                                protocol: "http",
                                host: hostname,
                                pathname:
                        "/AQUARIUS/Publish/V2/GetTimeSeriesDescriptionList"
                            });
                else if (error === 503)
                    throw 'Received HTTP error 503 ' +
                    '"Service Unavailable" from AQUARIUS; the ' +
                    'server might be down';
                else
                    throw error;
            });
    } // getTimeSeriesDescription

} // AQUARIUS

}
