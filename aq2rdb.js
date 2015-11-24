/**
 * @fileOverview A Web service to map AQUARIUS, time series data
 *               requests to USGS-variant RDB files.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

'use strict';
var http = require('http');
var httpdispatcher = require('httpdispatcher');
var url = require('url');
/**
   @see https://github.com/caolan/async
*/
var async = require('async');
var fs = require('fs');

/**
   @description The aq2rdb Web service name.
*/
var PACKAGE_NAME = 'aq2rdb';

/**
   @description The port the aq2rdb service listens on.
*/
var PORT = 8081;

/**
   @description AQUARIUS host.
*/
var AQUARIUS_HOSTNAME = 'nwists.usgs.gov';

/**
   @description AQUARIUS Web services path prefix.
*/
var AQUARIUS_PREFIX = '/AQUARIUS/Publish/V2/';

/**
   @description Consolidated error message writer. Writes message in
                a single-line, RDB comment.
*/ 
function rdbMessage(response, statusCode, message) {
    var statusMessage = '# ' + PACKAGE_NAME + ': ' + message;

    response.writeHead(statusCode, statusMessage,
                       {'Content-Length': statusMessage.length,
                        'Content-Type': 'text/plain'});
    response.end(statusMessage);
}

/**
   @description Convert an ISO 8601 extended format, date string to
                basic format.
*/
function toBasicFormat(s) {
    return s.replace('T', ' ').replace(/\.\d*/, '');
}

/**
   @description Convert AQUARIUS TimeSeriesPoint.Timestamp data type
                to common NWIS date type.
*/
function toNWISFormat(timestamp) {
    var date;

    try {
        date = new Date(timestamp);
    }
    catch (error) {
        throw error;
    }

    return timestamp.split('T')[0].replace(/-/g, '');
} // toNWISFormat

/**
   @description Create a valid HTTP query field/value pair substring.
*/ 
function bind(field, value) {
    if (value === undefined) {
        return '';
    }
    return '&' + field + '=' + value;
}

/**
   @description Error messager for JSON parse errors.
*/ 
function jsonParseErrorMessage(response, message) {
    rdbMessage(
        response, 502, 
        'While trying to parse a JSON response from ' +
            'AQUARIUS: ' + message
    );
}

/**
   @description LocationIdentifier object prototype.
   @class
*/
var LocationIdentifier = function (text) {
    var text = text;

    /**
       @description Parse agency code in AQUARIUS locationIdentifier.
    */
    this.agencyCode = function () {
        // if agency code delimiter ("-") is present in location
        // identifier
        if (text.search('-') === -1) {
            return 'USGS';    // default agency code
        }
        else {
            // parse (agency code, site number) embedded in
            // locationIdentifier
            var s = text.split('-');
            return s[1];
        }
    }

    /**
       @description Parse site number in AQUARIUS LocationIdentifier.
    */
    this.siteNumber = function () {
        // if agency code delimiter ("-") is present in location
        // identifier
        if (text.search('-') === -1) {
            return text;
        }
        else {
            // parse (agency code, site number) embedded in
            // locationIdentifier
            var s = text.split('-');
            return s[0];
        }
    }

    /**
       @description Return string representation of LocationIdentifier
                    object.
    */
    this.toString = function () {
        return text;
    }

} // LocationIdentifier

/**
   @description Call a REST Web service with a query; send response
                via a callback.
*/
function httpQuery(host, path, field, callback) {
    /**
       @description Handle response from GetLocationData.
    */
    function httpQueryCallback(response) {
        var messageBody = '';

        // accumulate response
        response.on(
            'data',
            function (chunk) {
                messageBody += chunk;
            });

        response.on('end', function () {
            callback(null, messageBody);
            return;
        });
    }
    
    path += '?';                // HTTP query string separator
    // bind HTTP query, field/value pairs
    for (var name in field) {
        path += bind(name, field[name]);
    }

    var request = http.request({
        host: host,
        path: path
    }, httpQueryCallback);

    /**
       @description Handle service invocation errors.
    */
    request.on('error', function (error) {
        callback(error);
        return;
    });

    request.end();
} // httpQuery

/**
   @description Call GetAQToken service to get AQUARIUS authentication
                token.
*/
function getAQToken(userName, password, callback) {

    if (userName === undefined) {
        callback('Required field "userName" is missing');
        return;
    }

    if (userName === '') {
        callback('Required field "userName" must have a value');
        return;
    }

    if (password === undefined) {
        callback('Required field "password" is missing');
        return;
    }

    if (password === '') {
        callback('Required field "password" must have a value');
        return;
    }

    /**
       @description GetAQToken service response callback.
    */
    function getAQTokenCallback(response) {
        var messageBody = '';

        // accumulate response
        response.on('data', function (chunk) {
            messageBody += chunk;
        });

        // Response complete; token received.
        response.on('end', function () {
            callback(null, messageBody);
            return;
        });
    } // getAQTokenCallback

    /**
       @description GetAQToken service request for AQUARIUS
                    authentication token needed for AQUARIUS API.
    */
    var path = '/services/GetAQToken?' +
        bind('userName', userName) +
        bind('password', password) +
        bind('uriString',
             'http://' + AQUARIUS_HOSTNAME + '/AQUARIUS/');
    var request = http.request({
        host: 'cidasdqaasaq2rd.cr.usgs.gov',
        port: '8080',
        path: path
    }, getAQTokenCallback);

    /**
       @description Handle GetAQToken service invocation errors.
    */
    request.on('error', function (error) {
        var statusMessage;

        if (error.message === 'connect ECONNREFUSED') {
            callback('Could not connect to GetAQToken service for ' +
                     'AQUARIUS authentication token');
        }
        else {
            callback(error);
        }
        return;
    });

    request.end();
} // getAQToken

/**
   @description Call AQUARIUS GetTimeSeriesCorrectedData Web service.
*/
function getTimeSeriesCorrectedData(
    token, timeSeriesUniqueId, queryFrom, queryTo, callback
) {
    /**
       @description Handle response from GetTimeSeriesCorrectedData.
    */
    function getTimeSeriesCorrectedDataCallback(response) {
        var messageBody = '';
        var timeSeriesCorrectedData;

        // accumulate response
        response.on(
            'data',
            function (chunk) {
                messageBody += chunk;
            });

        response.on('end', function () {
            callback(null, messageBody);
            return;
        });
    } // getTimeSeriesCorrectedDataCallback

    var path = AQUARIUS_PREFIX + 'GetTimeSeriesCorrectedData?' +
        bind('token', token) + bind('format', 'json') +
        bind('TimeSeriesUniqueId', timeSeriesUniqueId) +
        bind('QueryFrom', queryFrom) + bind('QueryTo', queryTo);

    var request = http.request({
        host: AQUARIUS_HOSTNAME,
        path: path
    }, getTimeSeriesCorrectedDataCallback);

    /**
       @description Handle GetTimeSeriesCorrectedData service
                    invocation errors.
    */
    request.on('error', function (error) {
        callback(error);
        return;
    });

    request.end();
} // getTimeSeriesCorrectedData

/**
   @description Call AQUARIUS GetLocationData Web service.
*/
function getLocationData(token, locationIdentifier, callback) {
    /**
       @description Handle response from GetLocationData.
    */
    function getLocationDataCallback(response) {
        var messageBody = '';

        // accumulate response
        response.on(
            'data',
            function (chunk) {
                messageBody += chunk;
            });

        response.on('end', function () {
            callback(null, messageBody);
            return;
        });
    }
    
    var path = AQUARIUS_PREFIX +
        'GetLocationData?' +
        bind('token', token) +
        bind('format', 'json') +
        bind('LocationIdentifier', locationIdentifier);

    var request = http.request({
        host: AQUARIUS_HOSTNAME,
        path: path                
    }, getLocationDataCallback);

    /**
       @description Handle GetTimeSeriesDescriptionList service
                    invocation errors.
    */
    request.on('error', function (error) {
        callback(error);
        return;
    });

    request.end();
} // getLocationData

/**
   @description Create RDB header block.
*/
function rdbHeader(
    agencyCode, siteNumber, stationName, timeZone, dstFlag,
    subLocationIdentifer, range
) {
    // some convoluted syntax for "now"
    var retrieved = toBasicFormat((new Date()).toISOString());

    // TODO: On Tue, Nov 10, 2015 at 4:16 PM, Brad Garner
    // <bdgarner@usgs.gov> said:
    // 
    // Andy,
    // I know I've mentioned before we consider going to a release
    // without all of these, and then let aggressive testing find the
    // gaps.  I still think that's a fine idea that aligns with the
    // spirit of minimally viable product.
    // 
    // How do we do this?  Here's an example.
    // 
    // Consider RNDARY="2222233332".  There is nothing like this
    // easily available from AQUARIUS API. Yet, AQ API does have the
    // new rounding specification, the next & improved evolution in
    // how we think of rounding; it looks like SIG(3) as one example.
    // Facing this, I can see 3 approaches, in increasing order of
    // complexity:
    //   1) Just stop.  Stop serving RNDARY="foo", assuming most
    //      people "just wanted the data"
    //   2) New field. Replace RNDARY with a new element like
    //      RNDSPEC="foo", which simply relays the new AQUARIUS
    //      RoundingSpec.
    //   3) Backward compatibility. Write code that converts a AQ
    //      rounding spec to a 10-digit NWIS rounding array.  Painful,
    //      full of assumptions & edge cases.  But surely doable.
    //
    // In the agile and minimum-vial-product [sic] spirits, I'd
    // propose leaning toward (1) as a starting point.  As user
    // testing and interaction informs us to the contrary, consider
    // (2) or (3) for some fields.  But recognize that option (2) is
    // always the most expensive one, so we should do it judiciously
    // and only when it's been demonstrated there's a user story
    // driving it.
    //
    // The above logic should work for every field in this header
    // block.
    //
    // Having said all that, some fields are trivially easy to find in
    // the AQUARIUS API--that is, option (3) is especially easy, so
    // maybe just do them.  In increasing order of difficulty (and
    // therefore increasing degrees of warranted-ness):
    //
    //  - LOCATION NAME="foo"  ... This would be the
    //    SubLocationIdentifer in a GetTimeSeriesDescriptionList()
    //    call.
    //  - PARAMETER LNAME="foo" ... is just Parameter as returned by
    //    GetTimeSeriesDescriptionList()
    //  - STATISTIC LNAME="foo" ... is ComputationIdentifier +
    //    ComputationPeriodIdentifier in
    //    GetTimeSeriesDescriptionList(), although the names will
    //    shift somewhat from what they would have been in ADAPS which
    //    might complicate things.
    //  - DD LABEL="foo" ... Except for the confusing carryover of the
    //    DD semantic, this should just be some combination of
    //    Identifier + Label + Comment + Description from
    //    GetTimeSeriesDescriptionList().  How to combine them, I'm
    //    not sure, but it should be determinable
    //  - DD DDID="foo" ...  When and if the extended attribute
    //    ADAPS_DD is populated in GetTimeSeriesDescriptionList(),
    //    this is easily populated.  But I think we should wean people
    //    off this.
    //  - Note: Although AGING fields might seem simple at first blush
    //    (the Approvals[] struct from GetTimeSeriesCorrectedData())
    //    the logic for emulating this old ADAPS format likely would
    //    get messy in a hurry.
    var header = '# //UNITED STATES GEOLOGICAL SURVEY       ' +
        'http://water.usgs.gov/\n' +
        '# //NATIONAL WATER INFORMATION SYSTEM     ' +
        'http://water.usgs.gov/data.html\n' +
        '# //DATA ARE PROVISIONAL AND SUBJECT TO CHANGE UNTIL ' +
        'PUBLISHED BY USGS\n' +
        '# //RETRIEVED: ' + retrieved.substr(0, retrieved.length - 1) +
        ' UTC\n' +
        '# //FILE TYPE="NWIS-I DAILY-VALUES" ' +
        'EDITABLE=NO\n' +
        '# //STATION AGENCY="' + agencyCode +
        '" NUMBER="' + siteNumber + '       " ' +
        'TIME_ZONE="' + timeZone + '" DST_FLAG=' + dstFlag + '\n' +
        '# //STATION NAME="' + stationName + '"\n';

    // On Wed, Nov 11, 2015 at 4:31 PM, Scott Bartholoma
    // <sbarthol@usgs.gov> said:
    //
    // I think that "# //LOCATION NUMBER=0 NAME="Default"" would
    // change to:
    // 
    // # //SUBLOCATION NAME="sublocation name"
    // 
    // and would be omitted if it were the default sublocation and
    // had no name.
    if (subLocationIdentifer !== undefined) {
        header += '# //SUBLOCATION ID="' + subLocationIdentifer + '"\n';
    }

    // I would be against continuing the DDID field since only
    // migrated timeseries will have ADAPS_DD populated.  Instead
    // we should probably replace the "# //DD" lines with "#
    // //TIMESERIES" lines, maybe something like:
    // 
    // # //TIMESERIES IDENTIFIER="Discharge, ft^3/s@12345678"
    // 
    // and maybe some other information.

    header += '# //RANGE START="';
    if (range.start !== undefined) {
        header += range.start;
    }
    header += '"';

    header += ' END="';
    if (range.end !== undefined) {
        header += range.end;
    }
    header += '"\n';

    return header;
} // rdbHeader

/**
   @description Create RDB table heading (which is different than a
                header).
*/
function rdbHeading() {
    return 'DATE\tTIME\tVALUE\tREMARK\tFLAGS\tTYPE\tQA\n' +
        '8D\t6S\t16N\t1S\t32S\t1S\t1S\n';
} // rdbHeading

/**
   @description Create RDB, DV table row.
*/
function dvTableRow(timestamp, value, qualifiers, remarkCodes, qa) {
    var row = toNWISFormat(timestamp) +
        // TIME column will always be empty for daily values
        '\t\t' + value + '\t';

    // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
    // <sbarthol@usgs.gov> said:
    //
    // Remark will have to be derived from the Qualifier
    // section of the response. It will have begin and end
    // dates for various qualification periods.
    async.detect(qualifiers, function (qualifier, callback) {
        var pointTime, startTime, endTime;

        try {
            pointTime = new Date(timestamp);
        }
        catch (error) {
            throw error;
            return;
        }

        try {
            startTime = new Date(qualifier.StartTime);
        }
        catch (error) {
            throw error;
            return;
        }

        try {
            endTime = new Date(qualifier.EndTime);
        }
        catch (error) {
            throw error;
            return;
        }

        // if this daily value's date point intersects the qualifier
        // interval
        if (startTime <= pointTime && pointTime <= endTime) {
            if (remarkCodes[qualifier.Identifier] !== undefined) {
                row += remarkCodes[qualifier.Identifier].toLowerCase();
            }
            else {
                throw 'No remark code found for "' +
                    qualifier.Identifier + '"';
                return;
            }
            callback(true);
        }
    }, function (result) {
        row += '\t';
    });

    // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
    // <sbarthol@usgs.gov> said:
    //
    // I think some of what used to be flags are now
    // Qualifiers. Things like thereshold exceedances
    // (high, very high, low, very low, rapid
    // increace/decreast [sic], etc.). The users might
    // want you to put something in that column for the
    // Method and Grade sections of the response as well
    row += '\t' +

    // TODO: There is outstanding mail to Wade Walker
    // <walker@usgs.gov> about preserving the TYPE column (see also
    // excerpt from Scott Bartholoma's mail below).

    // On Tue, Sep 29, 2015 at 10:57 AM, Scott Bartholoma
    // <sbarthol@usgs.gov> said:
    //
    // Type I would put in something like "R" for raw and
    // "C" for corrected depending on which get method was
    // used. That is similar to what C (computed) and E
    // (Edited) meant for DV data in Adaps.  We don't
    // explicitly have the Meas, Edit, and Comp UV types
    // anymore, they are separate timeseries in AQUARIUS.
    '\tC\t' + qa + '\n';

    return row;
} // dvTableRow

/**
   @description GetDVTable service request handler.
*/
httpdispatcher.onGet(
    '/' + PACKAGE_NAME + '/GetDVTable',
    function (request, response) {
        var field, token, locationIdentifier, timeSeriesUniqueId;
        var remarkCodes;

        /**
           @see https://github.com/caolan/async
        */
        async.waterfall([
            /**
               @description Parse fields and values in GetDVTable URL.
            */
            function (callback) {
                // if this is a documentation request
                if (request.url === '/' + PACKAGE_NAME + '/GetDVTable') {
                    // read and serve the documentation page
                    fs.readFile('doc/GetDVTable.html', function (error, html) {
                        if (error) {
                            callback(error);
                            return;
                        }       
                        response.writeHeader(
                            200, {"Content-Type": "text/html"}
                        );  
                        response.end(html);
                    });
                    return;
                }

                try {
                    field = url.parse(request.url, true).query;
                }
                catch (error) {
                    callback(error);
                    return;
                }

                for (var name in field) {
                    if (name.match(/^(userName|password)$/)) {
                        // GetAQToken fields
                    }
                    else if (name === 'LocationIdentifier') {
                        locationIdentifier =
                            new LocationIdentifier(field.LocationIdentifier);
                    }
                    else if (name.match(
                        /^(Parameter|ComputationIdentifier|QueryFrom|QueryTo)$/
                    )) {
                        // AQUARIUS fields
                    }
                    else {
                        callback('Unknown field "' + name + '"');
                        return;
                    }
                }
                
                if (locationIdentifier === undefined) {
                    callback(
                        'Required field "LocationIdentifier" not found'
                    );
                    return;
                }

                callback(null); // proceed to next waterfall
            },
            /**
               @description Get AQUARIUS authentication token from
                            GetAQToken service.
            */
            function (callback) {
                try {
                    getAQToken(
                        field.userName, field.password, callback
                    );
                }
                catch (error) {
                    // abort & pass "error" to final callback
                    callback(error);
                    return;
                }
                // no callback here, because it is passed to
                // getAQToken(), and called from there if successful
            },
            /**
               @description Receive AQUARIUS authentication token from
                            GetAQToken service.
            */
            function (messageBody, callback) {
                token = messageBody;
                callback(null);
            },
            /**
               @description Query AQUARIUS
                            GetTimeSeriesDescriptionList service to
                            get list of AQUARIUS, time series
                            UniqueIds related to aq2rdb, GetDVTable
                            location and parameter.
            */
            function (callback) {
                try {
                    httpQuery(
                        AQUARIUS_HOSTNAME,
                        AQUARIUS_PREFIX + 'GetTimeSeriesDescriptionList',
                        {token: token, format: 'json',
                         LocationIdentifier: locationIdentifier.toString(),
                         Parameter: field.Parameter,
                         ComputationIdentifier: field.ComputationIdentifier,
                         ComputationPeriodIdentifier: 'Daily',
                         ExtendedFilters:
                         '[{FilterName:ACTIVE_FLAG,FilterValue:Y}]'},
                        callback
                    );
                }
                catch (error) {
                    callback(error);
                    return;
                }
            },
            /**
               @description Receive response from AQUARIUS
                            GetTimeSeriesDescriptionList, then parse
                            list of related TimeSeriesDescriptions to
                            query AQUARIUS GetTimeSeriesCorrectedData
                            service.
            */
            function (messageBody, callback) {
                var timeSeriesDescriptionListServiceResponse;

                try {
                    timeSeriesDescriptionListServiceResponse =
                        JSON.parse(messageBody);
                }
                catch (error) {
                    callback(error);
                    return;
                }

                callback(
                 null,
                 timeSeriesDescriptionListServiceResponse.TimeSeriesDescriptions
                );
            },
            /**
               @description For each AQUARIUS time series description,
                            query GetTimeSeriesCorrectedData to get
                            related daily values.
            */
            function (timeSeriesDescriptions, callback) {
                switch (timeSeriesDescriptions.length) {
                case 0:
                    // TODO: error callback
                    break;
                case 1:
                    timeSeriesUniqueId =
                        timeSeriesDescriptions[0].UniqueId;
                    break;
                default:
                    /**
                       @description Filter out set of primary time
                                    series.
                    */
                    async.filter(
                        timeSeriesDescriptions,
                        /**
                           @description Primary time series filter
                                        iterator function.
                        */
                        function (timeSeriesDescription, callback) {
                            /**
                               @description Detect
                                            {"Name": "PRIMARY_FLAG",
                                             "Value": "Primary"} in
                                       TimeSeriesDescription.ExtendedAttributes
                            */
                            async.detect(
                                timeSeriesDescription.ExtendedAttributes,
                                function (extendedAttribute, callback) {
                                    // if this time series description
                                    // is (hopefully) the (only)
                                    // primary
                                    if (extendedAttribute.Name ===
                                        'PRIMARY_FLAG'
                                        &&
                                        extendedAttribute.Value ===
                                        'Primary') {
                                        callback(true);
                                    }
                                    else {
                                        callback(false);
                                    }
                                },
                                function (result) {
                                    // notify async.filter that we...
                                    if (result === undefined) {
                                        // ...did not find a primary
                                        // time series description
                                        callback(false);
                                    }
                                    else {
                                        // ...found a primary time
                                        // series description
                                        callback(true);
                                    }
                                }
                            );
                        },
                        function (primaryTimeSeriesDescriptions) {
                            // if there is 1-and-only-1 primary time
                            // series description
                            if (primaryTimeSeriesDescriptions.length ===
                                1) {
                                timeSeriesUniqueId =
                                    timeSeriesDescriptions[0].UniqueId;
                            }
                            else {
                                // TODO: we should probably defer
                                // production of header and heading
                                // until after this check.

                                // raise error
                                callback(
                                    'More than 1 primary time ' +
                                        'series found for "' +
                                        locationIdentifier.toString() +
                                        '"'
                                );
                            }
                        }
                    ); // async.filter
                } // switch (timeSeriesDescriptions.length)
                callback(null);
            },
            /**
               @description Query GetLocationData service to obtain
                            site name.
            */
            function (callback) {
                try {
                    httpQuery(
                        'waterservices.usgs.gov', '/nwis/site/',
                        {format: 'rdb',
                         sites: locationIdentifier.toString(),
                         siteOutput: 'expanded'}, callback
                    );
                }
                catch (error) {
                    callback(error);
                    return;
                }
            },
            /**
               @description Receive and parse response from
                            GetLocationData.
            */
            function (messageBody, callback) {
                var stationNm, tzCd, localTimeFg;

                // TODO: here we're parsing RDB, which is messy, and
                // would be nice to encapsulate.
                try {
                    // parse (station_nm,tz_cd,local_time_fg) from RDB
                    // response
                    var row = messageBody.split('\n');
                    // RDB column names
                    var columnName = row[row.length - 4].split('\t');
                    // site column values are in last row of table
                    var siteField = row[row.length - 2].split('\t');

                    // values that are used in the aq2rdb RDB header
                    stationNm =
                        siteField[columnName.indexOf('station_nm')];
                    tzCd = siteField[columnName.indexOf('tz_cd')];
                    localTimeFg =
                        siteField[columnName.indexOf('local_time_fg')];
                }
                catch (error) {
                    callback(error);
                    return;
                }
                callback(null, stationNm, tzCd, localTimeFg);
            },
            /**
               @description Write RDB header and heading.
            */
            function (stationNm, tzCd, localTimeFg, callback) {
                async.series([
                    /**
                       @description Write HTTP response header.
                    */
                    function (callback) {
                        response.writeHead(
                            200, {"Content-Type": "text/plain"}
                        );
                        callback(null);
                    },
                    /**
                       @description Write RDB header to HTTP response.
                    */
                    function (callback) {
                        var start, end;

                        if (field.QueryFrom !== undefined) {
                            start = toNWISFormat(field.QueryFrom);
                        }

                        if (field.QueryTo !== undefined) {
                            end = toNWISFormat(field.QueryTo);
                        }

                        var header = rdbHeader(
                            locationIdentifier.agencyCode(),
                            locationIdentifier.siteNumber(),
                            stationNm, tzCd, localTimeFg,
                            field.SubLocationIdentifer,
                            {start: start, end: end}
                        );
                        response.write(header, 'ascii');
                        callback(null);
                    },
                    /**
                       @description Write RDB heading (a different
                                    thing than RDB header, above) to
                                    HTTP response.
                    */
                    function (callback) {
                        response.write(rdbHeading(), 'ascii');
                        callback(null);
                    }
                ]);
                callback(null);
            },
            /**
               @description Request remark codes from AQUARIUS.
            */
            // TODO: this is fairly kludgey, because remark codes
            // might not be required for every DV interval; try to
            // nest in a conditional eventually.
            function (callback) {
                try {
                    httpQuery(
                        AQUARIUS_HOSTNAME,
                        AQUARIUS_PREFIX + 'GetQualifierList/',
                        {token: token,
                         format: 'json'}, callback
                    );
                }
                catch (error) {
                    callback(error);
                    return;
                }
            },
            /**
               @description Receive remark codes from AQUARIUS.
            */
            function (messageBody, callback) {
                var qualifierListServiceResponse;

                try {
                    qualifierListServiceResponse =
                        JSON.parse(messageBody);
                }
                catch (error) {
                    callback(error);
                    return;
                }

                // if we didn't get the remark codes domain table
                if (qualifierListServiceResponse === undefined) {
                    callback(
                        'Could not get remark codes from http://' +
                            AQUARIUS_HOSTNAME + AQUARIUS_PREFIX +
                            'GetQualifierList/'
                    );
                    return;
                }

                // put remark codes in an array for faster access later
                remarkCodes = new Array();
                async.each(
                    qualifierListServiceResponse.Qualifiers,
                    function (qualifierMetadata, callback) {
                        remarkCodes[qualifierMetadata.Identifier] =
                            qualifierMetadata.Code;
                        callback(null);
                    }
                );

                // proceed to next waterfall
                callback(null);
            },
            /**
               @description Query AQUARIUS GetTimeSeriesCorrectedData
                            to get related daily values.
            */
            function (callback) {
                try {
                    getTimeSeriesCorrectedData(
                        token, timeSeriesUniqueId,
                        field.QueryFrom, field.QueryTo, callback
                    );
                }
                catch (error) {
                    callback(error);
                    return;
                }
            },
            /**
               @description Parse AQUARIUS
                            TimeSeriesDataServiceResponse received
                            from GetTimeSeriesCorrectedData service.
            */
            function (messageBody, callback) {
                var timeSeriesDataServiceResponse;

                try {
                    timeSeriesDataServiceResponse =
                        JSON.parse(messageBody);
                }
                catch (error) {
                    callback(error);
                    return;
                }

                callback(null, timeSeriesDataServiceResponse);
            },
            /**
               @description Write each RDB row to HTTP response.
            */
            function (timeSeriesDataServiceResponse, callback) {
                async.each(
                    timeSeriesDataServiceResponse.Points,
                    /**
                       @description Write an RDB row for one time series
                       point.
                    */
                    function (timeSeriesPoint, callback) {
                        response.write(
                            dvTableRow(
                                timeSeriesPoint.Timestamp,
                                timeSeriesPoint.Value.Numeric.toString(),
                                timeSeriesDataServiceResponse.Qualifiers,
                                remarkCodes,
          timeSeriesDataServiceResponse.Approvals[0].LevelDescription.charAt(0)
                            ),
                            'ascii'
                        );
                        callback(null);
                    }
                );
                callback(null);
            }],
        /**
           @description node-async error handler function for
                        outer-most, GetDVTable async.waterfall
                        function.
        */
        function (error) {
            if (error) {
                var statusMessage =
                    '# ' + PACKAGE_NAME + ': ' + error;
                response.write(statusMessage, 'ascii');
            }
            response.end();
        }
      );
    }
); // GetDVTable

/**
   @description GetUVTable service request handler.
*/
httpdispatcher.onGet(
    '/' + PACKAGE_NAME + '/GetUVTable',
    function (request, response) {
        var field;
        var token;

        /**
           @see https://github.com/caolan/async
        */
        async.waterfall([
            /**
               @description Parse fields and values in GetUVTable URL.
            */
            function (callback) {
                try {
                    field = url.parse(request.url, true).query;
                }
                catch (error) {
                    callback(error);
                    return;
                }

                for (var name in field) {
                    if (name.match(/^(userName|password)$/)) {
                        // GetAQToken fields
                    }
                    else if (name === 'LocationIdentifier') {
                        locationIdentifier = new LocationIdentifier(name);
                    }
                    else if (name.match(/^(Parameter|QueryFrom|QueryTo)$/)) {
                        // AQUARIUS fields
                    }
                    else {
                        callback(new Error('Unknown field "' + name + '"'));
                        return;
                    }
                }
                callback(null); // proceed to next waterfall
            },
            /**
               @description Get AQUARIUS authentication token from
                            GetAQToken service.
            */
            function (callback) {
                try {
                    getAQToken(
                        field.userName, field.password, callback
                    );
                }
                catch (error) {
                    // abort & pass "error" to final callback
                    callback(error);
                }
                // no callback here, because it is passed to
                // getAQToken(), and called from there if successful
            },
            function (messageBody, callback) {
                token = messageBody;
            }
        ],
        /**
           @description node-async error handler function for
                        outer-most, GetUVTable async.waterfall
                        function.
        */
        function (error) {
            if (error) {
                rdbMessage(response, 400, error);
                return;
            }
            else {
                response.end();
            }
        }
        ); // async.waterfall
    }
); // GetUVTable

/**
   @description Service dispatcher.
*/ 
function handleRequest(request, response) {
    try {
        httpdispatcher.dispatch(request, response);
    }
    catch (error) {
        // put error message in an RDB comment line
        var statusMessage = '# ' + PACKAGE_NAME + ': ' + error;

        // TODO: need to make "statusCode" value [1st writeHead()
        // argument] more robust here
        response.writeHead(200, statusMessage,
                           {'Content-Length': statusMessage.length,
                            'Content-Type': 'text/plain'});
        response.end(statusMessage, 'ascii');
    }
}

/**
   @description Create HTTP server to host the service.
*/ 
var server = http.createServer(handleRequest);

/**
   @description Start listening for requests.
*/ 
server.listen(PORT, function () {
    console.log(PACKAGE_NAME + ': Server listening on: http://localhost:' +
                PORT.toString());
});
