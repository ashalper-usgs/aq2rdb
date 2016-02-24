/**
 * @fileOverview USGS RDB utility functions.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 * @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>
 *
 * @see <a href="https://sites.google.com/a/usgs.gov/nwis_integrator/data_retrieval/cli/aqts2rdb">aqts2rdb</a>.
 */

var moment = require('moment');
var sprintf = require("sprintf-js").sprintf;

var rdb = module.exports = {
    /**
       @function Node.js emulation of legacy NWIS, NW_RDB_HEADER(),
                 Fortran subroutine: "Write the rdb header lines to an
                 rdb file".
       @param response {object} HTTP response object to write to.
    */
    header: function (response) {
        response.write(
            '# //UNITED STATES GEOLOGICAL SURVEY ' +
                '      http://water.usgs.gov/\n' +
                '# //NATIONAL WATER INFORMATION SYSTEM ' +
                '    http://water.usgs.gov/data.html\n' +
                '# //DATA ARE PROVISIONAL AND SUBJECT TO ' +
                'CHANGE UNTIL PUBLISHED BY USGS\n' +
        '# //RETRIEVED: ' + moment().format('YYYY-MM-DD HH:mm:ss') + '\n'
        );
    }, // header

    /**
       @function takes an input date with < 8 chars and fills it to 8 chars
       @param wyflag {Boolean} flag if Water Year
       @param begdat {string} input date (may be < 8 chars)
    */
    fillBegDate: function (wyflag, begdat) {
        var iyr, begdate;

        // convert date/times to 8 characters

        if (wyflag) {
            if (begdat.length > 4)
                begdate = begdat.substring(0, 4);
            else
                begdate = begdat;

            iyr = parseInt(begdate);
            if (iyr <= 0)
                begdate = '00000000';
            else
                begdate = sprintf("%4d1001", iyr - 1);
        }
        else {
            if (begdat.length > 8)
                begdate = begdat.substring(0, 8);
            else
                begdate = begdat;
        }

        begdate = sprintf("%8s").replace(' ', '0');

        return begdate;
    }, // fillBegDate

    /**
       @function Node.js emulation of legacy NWIS,
                 NW_RDB_FILL_BEG_DTM() Fortran subroutine: "takes an
                 input date/time and fills it out to 14 chars".

       @param wyflag {Boolean} flag if Water Year
       @param begdat {string} input date/time (may be < 14 chars)
    */
    fillBegDtm: function(wyflag, begdat) {
        var begdtm, iyr;

        // convert date/times to 14 characters

        if (wyflag) {           // output = "<year-1>1001"
            if (begdat.length > 4)      // trim to just the year
                begdtm = begdat.substring(0, 4);
            else
                begdtm = begdat;

            iyr = parseInt(begdtm); // convert to numeric form
            if (iyr <= 0)
                begdtm = "00000000000000";
            else
                // write year-1, month=Oct, day=01
                begdtm = sprintf("%4d1001000000", iyr - 1);

            // Handle beginning of period - needs to be all zeros
            if (begdtm.substring(0, 4) === "0000") begdtm = "00000000000000";
        }
        else {                  // regular year, not WY
            if (begdat.length > 14)
                begdtm = begdat.substring(0, 14);
            else
                begdtm = begdat;
        }

        begdtm = sprintf("%14s", begdtm).replace(' ', '0');

        return begdtm;      
    }, // fillBegDtm

    /**
       @function Node.js emulation of legacy NWIS,
                 NW_RDB_FILL_END_DTM() Fortran subroutine: "takes an
                 input date/time and fills it out to 14 chars".
       @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
       @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>
       @param wyflag {Boolean} flag if Water Year
       @param enddat {string} input date/time (may be < 14 chars)
    */
    fillEndDtm: function (wyflag, enddat) {
        var enddtm;

        // convert date/times to 14 characters

        if (wyflag) {           // output will be "<year>0930235959"

            if (enddat.length > 4)
                enddtm = enddat.substring(0, 4);
            else
                enddtm = enddat;

            enddtm = sprintf("%4s0930235959", enddtm);
            if (enddtm.substring(0, 4) === "9999")
                enddtm = "99999999999999"; // end of period is all nines
            
        }
        else {                  // output will be filled-out date/time

            if (enddat.length > 14)
                enddtm = enddat.substring(0, 14);
            else
                enddtm = enddat;

        }

        enddtm = sprintf("%14s", enddtm);
        if (enddtm.substring(8, 14) === ' ') {
            if (enddtm.substring(0, 8) === "99999999")
                enddtm = enddtm.substr(0, 8) +  "999999";
            else
                enddtm = enddtm.substr(0, 8) + "235959";
        }
        enddtm = enddtm.replace(' ', '0');
        return enddtm;
    } // fillEndDtm

} // rdb
