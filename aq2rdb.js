/**
 * @fileOverview A Web service to map AQUARIUS, time series data
 *               requests to USGS-variant RDB files.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

'use strict';

// Node.js modules
var async = require('async');
var commandLineArgs = require('command-line-args');
var fs = require('fs');
var http = require('http');
var httpdispatcher = require('httpdispatcher');
var moment = require('moment-timezone');
var path = require('path');
var querystring = require('querystring');
var sprintf = require("sprintf-js").sprintf;
var url = require('url');

// aq2rdb modules
var aquarius = require('./aquarius');
var fdvrdbout = require('./fdvrdbout').fdvrdbout;
var rdb = require('./rdb');
var rest = require('./rest');
var site = require('./site');

/**
   @description The Web service name is the script name without the
                ".js" suffix.
*/
var packageName = path.basename(process.argv[1]).slice(0, -3);

/**
   @description Domain of supported command line arguments.
   @see https://www.npmjs.com/package/command-line-args#synopsis
*/
var cli = commandLineArgs([
    /**
       @description Print version and exit.
    */
    { name: 'version', alias: 'v', type: Boolean, defaultValue: false },
    /**
       @description Enable logging.
    */
    { name: 'log', alias: 'l', type: Boolean, defaultValue: false },
    /**
       @description TCP/IP port that aq2rdb will listen on.
    */
    {name: 'port', alias: 'p', type: Number, defaultValue: 8081},
    /**
       @description DNS name of AQUARIUS Web service host.
    */
    {name: 'aquariusHostname', alias: 'a', type: String},
    /**
       @description AQUARIUS Web service host, service account user
                    name.
    */
    {name: 'aquariusUserName', type: String},
    /**
       @description AQUARIUS Web service host, service account
                    password.
    */
    {name: 'aquariusPassword', type: String},
    /**
       @description DNS name of aquarius-token Web service host.
    */
    {name: 'aquariusTokenHostname', alias: 't', type: String},
    /**
       @description DNS name of USGS Water Services Web service host.
    */
    {name: 'waterServicesHostname', alias: 'w', type: String}
]);

/**
   @description Set of successfully parsed command line options.
*/
var options = cli.parse();

/**
   @description A mapping of select NWIS time zone codes to IANA time
                zone names (referenced by moment-timezone
                module). This is not a complete enumeration of the
                time zones defined in the NWIS TZ table, but the time
                zone abbreviations known (presently) to be related to
                SITEFILE sites in NATDB.
   @constant
*/
var tzName = Object();
tzName['AFT'] =   {N: 'Asia/Kabul', Y: 'Asia/Kabul'};
tzName['AKST'] =  {N: 'Etc/GMT-9',  Y: 'America/Anchorage'};
tzName['AST'] =   {N: 'Etc/GMT-4',  Y: 'America/Glace_Bay'};
tzName['AWST'] =  {N: 'Etc/GMT+4',  Y: 'Australia/Perth'};
tzName['BT'] =    {N: 'Etc/GMT+3',  Y: 'Asia/Baghdad'};
tzName['CST'] =   {N: 'Etc/GMT-6',  Y: 'America/Chicago'};
tzName['DST'] =   {N: 'Etc/GMT+1',  Y: 'Etc/GMT+1'};
tzName['EET'] =   {N: 'Etc/GMT+2',  Y: 'Europe/Athens'};
tzName['EST'] =   {N: 'Etc/GMT-5',  Y: 'America/New_York'};
tzName['GMT'] =   {N: 'Etc/GMT+0',  Y: 'Europe/London'};
tzName['GST'] =   {N: 'Etc/GMT+10', Y: 'Pacific/Guam'};
tzName['HST'] =   {N: 'Etc/GMT-10', Y: 'HST'};
// NWIS "International Date Line, East"
tzName['IDLE'] =  {N: 'Etc/GMT+12', Y: 'Etc/GMT+12'};
// NWIS "International Date Line, West"
tzName['IDLW'] =  {N: 'Etc/GMT-12', Y: 'Etc/GMT-12'};
tzName['JST'] =   {N: 'Etc/GMT+9',  Y: 'Asia/Tokyo'};
tzName['MST'] =   {N: 'Etc/GMT-7',  Y: 'America/Denver'};
// moment-timezone has no support for UTC-03:30 (in the context of
// Northern Hemisphere summer), which would be the mapping of NWIS'
// (NST,N) [i.e., "Newfoundland Standard Time, local time not
// acknowledged"] SITEFILE predicate...
tzName['NST'] =   {N: 'UTC-03:30',  Y: 'America/St_Johns'};
tzName['NZT'] =   {N: 'Etc/GMT+12', Y: 'NZ'};
tzName['PST'] =   {N: 'Etc/GMT-8',  Y: 'America/Los_Angeles'};
// ...similarly, moment-timezone has no support for UTC+09:30 (in the
// context of Southern Hemisphere summer), which would be the mapping
// of NWIS' (SAT,N) [i.e., "South Australian Standard Time, local time
// not acknowledged"]
tzName['SAT'] =   {N: 'UTC+09:30',  Y: 'Australia/Adelaide'};
tzName['UTC'] =   {N: 'Etc/GMT+0',  Y: 'Etc/GMT+0'};
tzName['WAST'] =  {N: 'Etc/GMT+7',  Y: 'Australia/Perth'};
tzName['WAT'] =   {N: 'Etc/GMT+1',  Y: 'Africa/Bangui'};
tzName['ZP-11'] = {N: 'Etc/GMT-11', Y: 'Etc/GMT-11'};
tzName['ZP11'] =  {N: 'Etc/GMT+11', Y: 'Etc/GMT+11'};
tzName['ZP4'] =   {N: 'Etc/GMT+4',  Y: 'Etc/GMT+4'};
tzName['ZP5'] =   {N: 'Etc/GMT+5',  Y: 'Etc/GMT+5'};
tzName['ZP6'] =   {N: 'Etc/GMT+6',  Y: 'Etc/GMT+6'};

/**
   @function Error handler.
   @param {object} error "Error" object.
   @param {object} response IncomingMessage object created by http.Server.
*/ 
function handle(error, response) {
    var statusMessage, statusCode;

    /**
       @see https://nodejs.org/api/errors.html#errors_error_code
    */
    if (error.code === 'ECONNREFUSED') {
        statusMessage = '# ' + packageName +
            ': Connection error; a common cause of this ' +
            'is GetAQToken being unreachable';
        /**
           @description "Bad Gateway"
           @see http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
        */
        statusCode = 502;
    }
    else if (error instanceof ReferenceError) {
        statusMessage =
            '# ' + packageName + ': There is an undefined ' +
            'reference on the ' + packageName + ' server';
        statusCode = 500;

        if (options.log === true) {
            console.log(packageName + ': ' +
                        error.toString().replace(/: (\w+)/, ': "$1"'));
        }
    }
    else if (typeof error === 'string') {
        statusMessage = '# ' + packageName + ': ' + error;
        /**
           @default HTTP error status code.
           @todo It would be nice to refine this. Too generic now.
        */
        statusCode = 404;
    }
    else {
        statusMessage = '# ' + packageName + ': ' + error.message;
        /**
           @default HTTP error status code.
           @todo It would be nice to refine this. Too generic now.
        */
        statusCode = 404;
    }

    response.writeHead(statusCode, statusMessage,
                       {'Content-Length': statusMessage.length,
                        'Content-Type': 'text/plain'});
    response.end(statusMessage, 'ascii');
    return;
} // handle

/**
   @description Public functions.
*/
var aq2rdb = module.exports = {
    /**
       @function Convert AQUARIUS TimeSeriesPoint.Timestamp string to
                 a common NWIS date format.
       @public
       @param {string} timestamp AQUARIUS Timestamp string to convert.
    */
    toNWISDateFormat: function (timestamp) {
        var date = new Date(timestamp);

        return timestamp.split('T')[0].replace(/-/g, '');
    },

    /**
       @function Convert AQUARIUS TimeSeriesPoint.Timestamp string to
                 a common NWIS time format.
       @public
       @param {string} timestamp AQUARIUS Timestamp string to convert.
    */
    toNWISTimeFormat: function (timestamp) {
        var date = new Date(timestamp);

        return timestamp.split(/[T.]/)[1].replace(/:/g, '');
    },

    /**
       @function Convert AQUARIUS TimeSeriesPoint.Timestamp string to
                 a common NWIS datetime format.
       @public
       @param {string} timestamp AQUARIUS Timestamp string to convert.
    */
    toNWISDatetimeFormat: function (timestamp) {
        return aq2rdb.toNWISDateFormat(timestamp) +
            aq2rdb.toNWISTimeFormat(timestamp);
    },

    /**
       @function Create RDB header block.
       @public
       @param {string} fileType Type of time series data (e.g. "NWIS-I
              DAILY-VALUES").
       @param {string} agencyCode Site agency code.
       @param {string} siteNumber Site number.
       @param {string} stationName Site name (a.k.a. station name).
       @param {string} timeZone Site time zone code.
       @param {string} dstFlag Site daylight saving time flag.
       @param {string} subLocationIdentifer Sublocation identifier.
       @param {object} range Time series query date range.
    */
    rdbHeaderBody: function (fileType, site, subLocationIdentifer, range) {
        /**
           @author <a href="mailto:bdgarner@usgs.gov">Bradley Garner</a>
    
           @todo
           
           Andy,
           I know I've mentioned before we consider going to a release
           without all of these, and then let aggressive testing find the
           gaps.  I still think that's a fine idea that aligns with the
           spirit of minimally viable product.
           
           How do we do this?  Here's an example.
           
           Consider RNDARY="2222233332".  There is nothing like this
           easily available from AQUARIUS API. Yet, AQ API does have the
           new rounding specification, the next & improved evolution in
           how we think of rounding; it looks like SIG(3) as one example.
           Facing this, I can see 3 approaches, in increasing order of
           complexity:
             1) Just stop.  Stop serving RNDARY="foo", assuming most
                people "just wanted the data"
             2) New field. Replace RNDARY with a new element like
                RNDSPEC="foo", which simply relays the new AQUARIUS
                RoundingSpec.
             3) Backward compatibility. Write code that converts a AQ
                rounding spec to a 10-digit NWIS rounding array.  Painful,
                full of assumptions & edge cases.  But surely doable.
          
           In the agile and minimum-vial-product [sic] spirits, I'd
           propose leaning toward (1) as a starting point.  As user
           testing and interaction informs us to the contrary, consider
           (2) or (3) for some fields.  But recognize that option (2) is
           always the most expensive one, so we should do it judiciously
           and only when it's been demonstrated there's a user story
           driving it.
          
           The above logic should work for every field in this header
           block.
          
           Having said all that, some fields are trivially easy to find in
           the AQUARIUS API--that is, option (3) is especially easy, so
           maybe just do them.  In increasing order of difficulty (and
           therefore increasing degrees of warranted-ness):
          
            - LOCATION NAME="foo"  ... This would be the
              SubLocationIdentifer in a GetTimeSeriesDescriptionList()
              call.
            - PARAMETER LNAME="foo" ... is just Parameter as returned by
              GetTimeSeriesDescriptionList()
            - STATISTIC LNAME="foo" ... is ComputationIdentifier +
              ComputationPeriodIdentifier in
              GetTimeSeriesDescriptionList(), although the names will
              shift somewhat from what they would have been in ADAPS which
              might complicate things.
            - DD LABEL="foo" ... Except for the confusing carryover of the
              DD semantic, this should just be some combination of
              Identifier + Label + Comment + Description from
              GetTimeSeriesDescriptionList().  How to combine them, I'm
              not sure, but it should be determinable
            - DD DDID="foo" ...  When and if the extended attribute
              ADAPS_DD is populated in GetTimeSeriesDescriptionList(),
              this is easily populated.  But I think we should wean people
              off this.
            - Note: Although AGING fields might seem simple at first blush
              (the Approvals[] struct from GetTimeSeriesCorrectedData())
              the logic for emulating this old ADAPS format likely would
              get messy in a hurry.
        */

        var header =
            '# //FILE TYPE="' + fileType + '" ' + 'EDITABLE=NO\n' +
            '# //STATION AGENCY="' + site.agencyCode +
            '" NUMBER="' + site.number + '       " ' +
            'TIME_ZONE="' + site.tzCode + '" DST_FLAG=' +
            site.localTimeFlag + '\n' +
            '# //STATION NAME="' + site.name + '"\n';
    
        /**
           @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>
    
           @since 2015-11-11T16:31-07:00
    
           @todo
    
           I think that "# //LOCATION NUMBER=0 NAME="Default"" would
           change to:
           
           # //SUBLOCATION NAME="sublocation name"
           
           and would be omitted if it were the default sublocation and
           had no name.
        */
        if (subLocationIdentifer !== undefined) {
            header += '# //SUBLOCATION ID="' + subLocationIdentifer + '"\n';
        }
    
        /**
           @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>
    
           @since 2015-11-11T16:31-07:00
    
           I would be against continuing the DDID field since only
           migrated timeseries will have ADAPS_DD populated.  Instead
           we should probably replace the "# //DD" lines with "#
           //TIMESERIES" lines, maybe something like:
           
           # //TIMESERIES IDENTIFIER="Discharge, ft^3/s@12345678"
           
           and maybe some other information.
        */
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
    } // rdbHeaderBody

}; // public functions

/**
   @function Error messager for JSON parse errors.
   @private
   @param {object} response IncomingMessage object created by http.Server.
   @param {string} message Error message to display in an RDB comment.
*/
function jsonParseErrorMessage(response, message) {
    var statusMessage = 'While trying to parse a JSON response ' +
        'from AQUARIUS: ' + message;

    response.writeHead(502, statusMessage,
                       {'Content-Length': statusMessage.length,
                        'Content-Type': 'text/plain'});
    response.write(statusMessage, 'ascii');
}

/**
   @description Detect presence of GetAQToken field.
   @private
*/
function isGetAQTokenField(name) {
    if (name == 'userName' || name == 'password')
        return true;
    else
        return false;
}

/**
   @description LocationIdentifier object prototype.
   @class
   @private
*/
var LocationIdentifier = function (text) {
    var text = text;

    /**
       @method Return agency code.
    */
    this.agencyCode = function () {
        // if agency code delimiter ("-") is present in location
        // identifier
        if (text.search('-') === -1) {
            /**
               @default Agency code.
            */
            return 'USGS';
        }
        else {
            // parse (agency code, site number) embedded in
            // locationIdentifier
            var s = text.split('-');
            return s[1];
        }
    }

    /**
       @method Return site number.
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
       @method Return string representation of this object.
    */
    this.toString = function () {
        return text;
    }

} // LocationIdentifier

/**
   @function Call GetAQToken service to get AQUARIUS authentication
             token.
   @private
   @param {string} hostname AQUARIUS TCP/IP host name.
   @param {string} userName AQUARIUS user name.
   @param {string} password AQUARIUS password.
   @param {function} callback Callback function to call if/when
          GetAQToken responds.
*/
function getAQToken(hostname, userName, password, callback) {

    if (hostname === undefined) {
        callback('Required field "hostname" not found');
        return;
    }

    if (hostname === '') {
        callback('Required field "hostname" must have a value');
        return;
    }

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
       @description GetAQToken service response callback.
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
            callback(null, messageBody);
            return;
        });
    } // getAQTokenCallback

    var port = '8080';
    var path = '/services/GetAQToken?';
    var uriString = 'http://' + hostname + '/AQUARIUS/';

    if (options.log === true) {
        console.log(
            packageName + ': querying http://' + hostname + ':' +
                port + path + '..., AQUARIUS server at ' + uriString
        );
    }

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
        host: options.aquariusTokenHostname,
        port: port,             // TODO: make a CLI parameter?
        path: path
    }, getAQTokenCallback);

    /**
       @description Handle GetAQToken service invocation errors.
    */
    request.on('error', function (error) {
        var statusMessage;

        if (options.log === true) {
            console.log(packageName + ': ' + error);
        }

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
   @function Check for documentation request, and serve documentation
             if appropriate.
   @private
   @param {string} url Endpoint URL.
   @param {string} name Endpoint name.
   @param {object} response Response object.
   @param {function} callback Callback function to call when complete.
*/
function docRequest(url, servicePath, response, callback) {
    // if this is a documentation request
    if (url === servicePath) {
        // read and serve the documentation page
        fs.readFile(
            'doc/' + path.basename(servicePath) + '.html',
            function (error, html) {
                if (error) {
                    callback(error);
                    return true;
                }       
                response.writeHeader(200, {"Content-Type": "text/html"});  
                response.end(html);
            }
        );
        return true;
    }
    else
        return false;
} // docRequest

/**
   @function Create RDB, DV table row.
   @private
   @param {string} timestamp AQUARIUS timestamp string.
   @param {object} value Time series daily value.
   @param {object} qualifiers AQUARIUS
          QualifierListServiceResponse.Qualifiers.
   @param {object} remarkCodes An array (as domain table) of daily
          values remark codes, indexed by AQUARIUS
          QualifierMetadata.Identifier.
   @param {string} qa QA code.
*/
function dvTableRow(timestamp, value, qualifiers, remarkCodes, qa) {
    // TIME column will always be empty for daily values
    var row = aq2rdb.toNWISDateFormat(timestamp) + '\t\t';

    if (value.Numeric !== undefined)
        row += value.Numeric.toString();

    row += '\t';

    /**
       @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>

       @since 2015-09-29T10:57-07:00

       Remark will have to be derived from the Qualifier section of
       the response. It will have begin and end dates for various
       qualification periods.
    */
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

    /**
       @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>

       @since 2015-09-29T10:57-07:00
      
       I think some of what used to be flags are now
       Qualifiers. Things like thereshold [sic] exceedances [sic]
       (high, very high, low, very low, rapid increace/decreast [sic],
       etc.). The users might want you to put something in that column
       for the Method and Grade sections of the response as well
    */
    row += '\t' +

    /**
       @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>

       @since 2015-09-29T10:57-07:00
      
       Type I would put in something like "R" for raw and "C" for
       corrected depending on which get method was used. That is
       similar to what C (computed) and E (Edited) meant for DV data
       in Adaps.  We don't explicitly have the Meas, Edit, and Comp UV
       types anymore, they are separate timeseries in AQUARIUS.
    */
    '\tC\t' + qa + '\n';

    return row;
} // dvTableRow

/**
   @function Patch up some obscure incompatibilities between NWIS's
             site time offset predicate and IANA time zone data (used
             by moment-timezone).
   @private
*/
function nwisVersusIANA(timestamp, name, tzCode, localTimeFlag) {
    var m = moment.tz(timestamp, name);
    var p = new Object();       // datetime point

    // if this site's time offset predicate is "local time not
    // acknowledged", and observes Newfoundland Standard Time or South
    // Australian Standard Time, and date point is within the
    // associated, effective daylight saving time interval
    if (localTimeFlag === 'N' &&
        (tzCode === 'NST' && m.zoneAbbr() === 'NDT' ||
         tzCode === 'SAT' && m.zoneAbbr() === 'ACDT')) {
        var t = new Date(point.Timestamp);
        // normalize time to UTC, then apply time zone offset from UTC
        var offset = name.replace('UTC', '');
        var invertedOffset =
            offset.replace('+', '-') || offset.replace('-', '+');
        var utc = new Date(t.toISOString().replace('Z', invertedOffset));
        var v = utc.toISOString().split(/[T.]/);

        // reformat ISO 8601 date/time to NWIS date/time format
        p.date = v[0].replace('-', '');
        p.time = v[1].replace(':', '');
        // use (non-IANA) name as time zone abbreviation
        p.tz = name;
    }
    else {
        // use IANA time zone data
        p.date = m.format('YYYYMMDD');
        p.time = m.format('hhmmss');
        p.tz = m.format('z');
    }

    return p;
} // nwisVersusIANA

/**
   @description AQUARIUS Web service login credentials object prototype.
   @class
   @private
*/
function AquariusCredentials (cli, http) {
    /**
       @todo Need some fallback logic here to read
             (hostname,userName,password) from encrypted configuration
             file if not provided as REST parameters in the service
             request, or on the command at service start-up.
    */
    // if any of AQUARIUS (hostname,userName,password) are
    // missing from the Web service request...
    if (http.hostname === undefined ||
        http.userName === undefined ||
        http.password === undefined) {
        // ...fall-back on the service start-up values
        this.hostname = cli.hostname;
        this.userName = cli.userName;
        this.password = cli.password;
    }
    else {
        // use the values provided in the HTTP query
        this.hostname = http.hostname;
        this.userName = http.userName;
        this.password = http.password;
    }
} // AquariusCredentials

/**
   @function Partial Node.js of legacy NWIS, RDB_OUT() Fortran
             subroutine: "Top-level routine for outputting rdb format
             data"
   @param datatyp      (inp - C*(*)) rating type
   @param rndsup       (inp - C*(*)) Y/N flag for rounding-suppressed
   @param inwyflag     (inp - C*(*)) Y/N flag for water-year
   @param incflag      (inp - C*(*)) Y/N flag for Computed DVs/Combined Datetimes (UVs)
   @param invflag      (inp - C*(*)) Y/N flag for verbose dates and times
   @param inagny       (inp - C*(*)) agency code
   @param instnid      (inp - C*(*)) station number
   @param INLOCNU      (inp - C*(*)) Location number
   @param INSTAT       (inp - C*(*)) Statistics code
   @param INTRANS      (inp - C*(*)) UV Transport code
   @param BEGDAT       (inp - C*(*)) begin date
   @param ENDDAT       (inp - C*(*)) end date
   @param IN_LOC_TZ_CD (inp - C*(*)) time zone code
   @param TITLLINE     (inp - C*(*)) title line (text)
   @return (Integer*4) returns the error code from modules called (0 IF all OK)
   @private
 */
function rdbOut(
    response, datatyp, rndsup, wyflag, cflag, vflag, inagny, instnid,
    locnu, instat, intrans, begdat, enddat, inLocTzCd, titlline
) {
    if (options.log)
        console.log(
            sprintf(
                "datatyp=%s, rndsup=%s, wyflag=%s, cflag=%s, " +
                    "vflag=%s, inagny=%s, instnid=%s, locnu=%s, " +
                    "instat=%s, intrans=%s, begdat=%s, enddat=%s, " +
                    "inLocTzCd=%s, titlline=%s",
                datatyp, rndsup, wyflag, cflag, vflag, inagny,
                instnid, locnu, instat, intrans, begdat, enddat,
                inLocTzCd, titlline
        )
    );

    // string
    var sopt, rdbfile, rtagny, sid;
    var parm, stat, transportCd, uvtyp, inguvtyp, mstyp;
    var mssav, vttyp, wltyp, pktyp, qwparm, qwmeth;
    var begdate, enddate, begdtm, enddtm, bctdtm, ectdtm;
    var cdate, ctime, tzCd, locTzCd, localTimeFg;

    // integer
    var ipu, irc, sensorTypeId, iyr, i;
    var one = 1, two = 2, three = 3;

    // Boolean
    var needstrt, first, uvtypPrompted;

    needstrt = false;
    uvtypPrompted = false;
    parm = undefined;
    locTzCd = inLocTzCd;
    if (locTzCd === undefined) locTzCd = 'LOC';

    if (intrans === undefined) {
        transportCd = undefined;
    }
    else {
        transportCd = intrans.charAt(0).toUpperCase();
    }

    // init control argument
    sopt = '10000000000000000000000000000000'.split('');
    if (datatyp.length > 2)
        datatyp = datatyp.substring(0, 2);
         
    datatyp = datatyp.toUpperCase();

    // convert agency to 5 characters - default to USGS
    if (inagny === undefined)
        rtagny = 'USGS';
    else {
        if (inagny.length > 5)
            rtagny = inagny.substring(0, 5);
        else
            rtagny = inagny;
        rtagny = sprintf("%-5s", inagny);       
    }
    // convert station to 15 characters
    if (instnid === undefined) {
        needstrt = true;
        if (datatyp === 'MS' || datatyp === 'PK' ||
            datatyp === 'WL' || datatyp === 'QW')
            sopt[4] = '1';
        else
            sopt[4] = '2';
    }
    else {
        if (instnid.length > 15)
            sid = instnid.substring(0, 15);
        else
            sid = instnid
        sid = sprintf("%-5s", sid);
    }

    // DD is ignored for data types MS, PR, WL, and QW

    if (datatyp !== 'MS' && datatyp !== 'PK' &&
        datatyp !== 'WL' && datatyp !== 'QW') {

        if ((datatyp !== 'VT') ||
            (datatyp === 'VT' && (locnu === undefined))) {
               needstrt = true;
               sopt[4] = '2';
        }
        else {
            parm = undefined;
        }

    }

    // further processing depends on data type

    if (datatyp === 'DV') {     // convert stat to 5 characters
        if (instat === undefined) {
            needstrt = true;
            sopt[7] = '1';
        }
        else {
            if (instat.length > 5)
                stat = instat.substring(0, 5);
            else
                stat = instat;
            stat = sprintf("%5s", stat).replace(' ', '0');
        }
    }

    if (datatyp === 'DV' || datatyp === 'DC' ||
        datatyp === 'SV' || datatyp === 'PK') {

        // convert dates to 8 characters
        if (begdat === undefined || enddat === undefined) {
            needstrt = true;
            if (wyflag)
                sopt[8] = '4';
            else
                sopt[9] = '3';
        }
        else {
            begdate = rdb.fillBegDate(wyflag, begdat);
            enddate = rdb.fillEndDate(wyflag, enddat);
        }

    }

    if (datatyp === 'UV') {

        uvtyp = instat.charAt(0);
        if (uvtyp === 'm' || uvtyp === 'n' || uvtyp === 'e' ||
            uvtyp === 'r' || uvtyp === 's' || uvtyp === 'c')
            uvtyp = uvtyp.toUpperCase();

        // convert date/times to 14 characters
        if (begdat === undefined || enddat === undefined) {
            needstrt = true;
            if (wyflag)
                sopt[8] = '4';
            else
                sopt[9] = '3';
        }
        else {
            begdtm = rdb.fillBegDtm(wyflag, begdat);
            enddtm = rdb.fillEndDtm(wyflag, enddat);
        }

    }

         // get data and output to files

         if (datatyp === 'DV')
             irc = fdvrdbout(
                 response, false, rndsup, vflag, cflag, rtagny, sid,
                 stat, begdate, enddate
             );
         else if (datatyp === 'UV') {
             /**
                @todo re-factor to an indexed array:
              */
            if (uvtyp === 'M') inguvtyp = "meas";
            if (uvtyp === 'N') inguvtyp = "msar";
            if (uvtyp === 'E') inguvtyp = "edit";
            if (uvtyp === 'R') inguvtyp = "corr";
            if (uvtyp === 'S') inguvtyp = "shift";
            if (uvtyp === 'C') inguvtyp = "da";

            irc = fuvrdbout(
                response, false, rndsup, cflag, vflag, rtagny, sid,
                inguvtyp, sensorTypeId, transportCd, begdtm, enddtm,
                locTzCd
            );

         }
/*
      !  close files and exit
997   s_mclos
      s_sclose (funit, 'keep')
      nw_disconnect
      GOTO 999

      !  bad return (do a generic error message)
998   irc = 3
      nw_error_handler (irc,'nwf_rdb_out','error',
     *     'doing something','something bad happened')

      !  Good return
999   nwf_rdb_out = irc
      RETURN
      END
*/
    return irc;
} // rdbOut

/**
   @description GetDVTable endpoint service request handler.
*/
httpdispatcher.onGet(
    '/' + packageName + '/GetDVTable',
    /**
       @callback
    */
    function (request, response) {
        var field, token, locationIdentifier, timeSeriesDescription;
        var remarkCodes;

        /**
           @see https://github.com/caolan/async
        */
        async.waterfall([
            /**
               @function Check for documentation request.
               @callback
            */
            function (callback) {
                if (docRequest(request.url, '/aq2rdb/GetDVTable',
                               response, callback))
                    return;
                callback(null);
            },
            /**
               @function Parse fields and values in GetDVTable URL.
               @callback
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
                    if (isGetAQTokenField(name)) {
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
                    callback('Required field "LocationIdentifier" not found');
                    return;
                }

                if (field.Parameter === undefined) {
                    callback('Required field "Parameter" not found');
                    return;
                }

                var aquariusCredentials = new AquariusCredentials(
                    {hostname: options.aquariusHostname,
                     userName: options.aquariusUserName,
                     password: options.aquariusPassword},
                    {hostname: field.hostname,
                     userName: field.userName,
                     password: field.password}
                );

                // proceed to next waterfall
                callback(null, aquariusCredentials);
            },
            /**
               @function Get AQUARIUS authentication token from
                         GetAQToken service.
               @callback
            */
            function (aquariusCredentials, callback) {
                try {
                    getAQToken(
                        aquariusCredentials.hostname,
                        aquariusCredentials.userName,
                        aquariusCredentials.password,
                        callback
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
               @function Receive AQUARIUS authentication token from
                         GetAQToken service.
               @callback
               @param {string} messageBody Message body of response
                      from GetAQToken.
               @param {function} callback async.waterfall() callback
                      function.
            */
            function (messageBody, callback) {
                token = messageBody;
                callback(null);
            },
            /**
               @function Query AQUARIUS GetTimeSeriesDescriptionList
                         service to get list of AQUARIUS, time series
                         UniqueIds related to aq2rdb, GetDVTable
                         location and parameter.
               @callback
               @param {function} callback async.waterfall() callback
                      function.
            */
            function (callback) {
                var obj =
                    {token: token, format: 'json',
                     LocationIdentifier: locationIdentifier.toString(),
                     Parameter: field.Parameter,
                     ComputationPeriodIdentifier: 'Daily',
                     ExtendedFilters:
                     '[{FilterName:ACTIVE_FLAG,FilterValue:Y}]'};

                if (field.ComputationIdentifier !== undefined)
                    obj.ComputationIdentifier = field.ComputationIdentifier;
                
                try {
                    rest.query(
                        options.aquariusHostname,
                        aquarius.PREFIX + 'GetTimeSeriesDescriptionList',
                        obj,
                        callback
                    );
                }
                catch (error) {
                    callback(error);
                    return;
                }
            },
            /**
               @function Receive response from AQUARIUS
                         GetTimeSeriesDescriptionList, then parse list
                         of related TimeSeriesDescriptions to query
                         AQUARIUS GetTimeSeriesCorrectedData service.
               @callback
               @param {string} messageBody Message body part of
                      HTTP response from GetTimeSeriesDescriptionList.
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
               @function For each AQUARIUS time series description,
                         query GetTimeSeriesCorrectedData to get
                         related daily values.
               @callback
            */
            function (timeSeriesDescriptions, callback) {
                timeSeriesDescription =
                    aquarius.distill(
                        timeSeriesDescriptions, locationIdentifier, callback
                    );

                if (options.log === true) {
                    console.log(
                        packageName + '.locationIdentifier: ' +
                            locationIdentifier
                    );
                }

                callback(
                    null, options.waterServicesHostname,
                    locationIdentifier.siteNumber(), options.log
                );
            },
            site.request,
            site.receive,
            /**
               @function Write RDB header and heading.
               @callback
            */
            function (site, callback) {
                async.series([
                    /**
                       @function Write HTTP response header.
                       @callback
                    */
                    function (callback) {
                        response.writeHead(
                            200, {"Content-Type": "text/plain"}
                        );
                        callback(null);
                    },
                    /**
                       @function Write RDB header to HTTP response.
                       @callback
                    */
                    function (callback) {
                        rdb.header(response);
                        callback(null);
                    },
                    /**
                       @function Write RDB body to HTTP response.
                       @callback
                    */
                    function (callback) {
                        var start, end;

                        if (field.QueryFrom !== undefined) {
                            start = aq2rdb.toNWISDateFormat(field.QueryFrom);
                        }

                        if (field.QueryTo !== undefined) {
                            end = aq2rdb.toNWISDateFormat(field.QueryTo);
                        }

                        var header = aq2rdb.rdbHeaderBody(
                            'NWIS-I DAILY-VALUES', site,
                            timeSeriesDescription.SubLocationIdentifer,
                            {start: start, end: end}
                        );
                        response.write(header, 'ascii');
                        callback(null);
                    },
                    /**
                       @function Write RDB heading (a different thing
                                 than RDB header, above) to HTTP
                                 response.
                       @callback
                    */
                    function (callback) {
                        response.write(
                            'DATE\tTIME\tVALUE\tREMARK\tFLAGS\tTYPE\tQA\n' +
                                '8D\t6S\t16N\t1S\t32S\t1S\t1S\n', 'ascii'
                        );
                        callback(null);
                    }
                ]);
                callback(null);
            },
            /**
               @function Request remark codes from AQUARIUS.
               @callback
               @todo This is fairly kludgey, because remark codes
                     might not be required for every DV interval; try
                     to nest in a conditional eventually.
            */
            function (callback) {
                try {
                    rest.query(
                        options.aquariusHostname,
                        aquarius.PREFIX + 'GetQualifierList/',
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
               @function Receive remark codes from AQUARIUS.
               @callback
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
                            options.aquariusHostname + aquarius.PREFIX +
                            'GetQualifierList/'
                    );
                    return;
                }

                // put remark codes in an array for faster access later
                remarkCodes = new Array();
                async.each(
                    qualifierListServiceResponse.Qualifiers,
                    /**
                       @callback
                    */
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
               @function Query AQUARIUS GetTimeSeriesCorrectedData
                         to get related daily values.
               @callback
            */
            function (callback) {
                try {
                    aquarius.getTimeSeriesCorrectedData(
                        token, timeSeriesDescription.UniqueId,
                        field.QueryFrom, field.QueryTo, callback
                    );
                }
                catch (error) {
                    callback(error);
                    return;
                }
            },
            aquarius.parseTimeSeriesDataServiceResponse,
            /**
               @function Write each RDB row to HTTP response.
               @callback
            */
            function (timeSeriesDataServiceResponse, callback) {
                async.each(
                    timeSeriesDataServiceResponse.Points,
                    /**
                       @description Write an RDB row for one time series
                                    point.
                       @callback
                    */
                    function (timeSeriesPoint, callback) {
                        response.write(
                            dvTableRow(
                                timeSeriesPoint.Timestamp,
                                timeSeriesPoint.Value,
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
           @callback
        */
        function (error) {
            if (error) {
                handle(error, response);
            }
            response.end();
        }
      );
    }
); // GetDVTable

/**
   @description GetUVTable endpoint service request handler.
*/
httpdispatcher.onGet(
    '/' + packageName + '/GetUVTable',
    /**
       @callback
    */
    function (request, response) {
        var field, token, locationIdentifier, site, parameter;

        async.waterfall([
            function (callback) {
                if (docRequest(request.url, '/aq2rdb/GetUVTable',
                               response, callback))
                    return;
                callback(null);
            },
            function (callback) {
                console.log('2nd callback');
                callback(null);
            }
        ],
            /**
               @description node-async error handler function for
                            outer-most, GetUVTable async.waterfall
                            function.
               @callback
            */
            function (error) {
                if (error) {
                    handle(error, response);
                }
                response.end();
            }
        );
    }
); // GetUVTable

/**
   @description aq2rdb endpoint service request handler.
*/
httpdispatcher.onGet(
    '/' + packageName,
    /**
       @callback
    */
    function (request, response) {
        var token, locationIdentifier, site, parameter;

        async.waterfall([
            function (callback) {
                if (docRequest(request.url, '/aq2rdb', response, callback))
                    return;
                callback(null);
            },
            function (callback) {
                var field;

                try {
                    field = url.parse(request.url, true).query;
                }
                catch (error) {
                    callback(error);
                    return;
                }

                for (var name in field) {
                    if (isGetAQTokenField(name)) {
                        // GetAQToken fields
                    }
                    else if (name.match(/^(a|p|t|s|n|b|e)$/)) {
                        // aq2rdb fields
                    }
                    else {
                        callback('Unknown field "' + name + '"');
                        return;
                    }
                }

                callback(
                    null, field.t, field.a, field.n, field.p, field.s,
                    field.b, field.e
                );
            },
            function (
                datatyp, agency, station, parm, stat, begdat, enddat,
                callback
            ) {
                rdbOut(
                    response, datatyp, undefined, undefined,
                    undefined, undefined, agency, station, undefined,
                    stat, undefined, begdat, enddat, undefined,
                    undefined
                );
                callback(null);
            }
        ],
            /**
               @description node-async error handler function for
                            outer-most, GetUVTable async.waterfall
                            function.
               @callback
            */
            function (error) {
                console.log('error callback');
                if (error) {
                    handle(error, response);
                }
                response.end();
            }
        );
    }
); // GetUVTable

/**
   @description Service dispatcher.
*/ 
function handleRequest(request, response) {
    try {
        if (options.log === true) {
            console.log(
                packageName + '.handleRequest.request.url: ' +
                    request.url
            );
        }
        httpdispatcher.dispatch(request, response);
    }
    catch (error) {
        handle(error, response);
    }
}

/**
   @description Check for "version" CLI option.
*/
if (options.version === true) {
    fs.readFile('package.json', function (error, json) {
        if (error) {
            callback(error);
            return;
        }
   
        var pkg;
        try {
            pkg = JSON.parse(json);
        }
        catch (error) {
            if (options.log === true) {
                console.log(packageName + ': ' + error);
            }
            return;
        }

        console.log(pkg.version);
    });
}
else {
    /**
       @description Create HTTP server to host the service.
    */
    var server = http.createServer(handleRequest);

    /**
       @description Start listening for requests.
    */ 
    server.listen(options.port, function () {
        if (options.log === true) {
            console.log(
                packageName + ': Server listening on: http://localhost:' +
                    options.port.toString()
            );
        }
    });
}

/**
   @description Export module's private functions to test harness
                only.
   @see http://engineering.clever.com/2014/07/29/testing-private-functions-in-javascript-modules/
*/
if (process.env.NODE_ENV === 'test') {
    module.exports._private = {
        handle: handle,
        jsonParseErrorMessage: jsonParseErrorMessage,
        getAQToken: getAQToken,
        docRequest: docRequest,
        dvTableRow: dvTableRow,
        nwisVersusIANA: nwisVersusIANA
    };
}
