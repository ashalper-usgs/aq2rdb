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
       @param response  {object} HTTP response object to write to.
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
       @param wyflag  {Boolean} flag if Water Year
       @param begdat  {string} input date (may be < 8 chars)
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
    } // fillBegDate

} // rdb
