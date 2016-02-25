/**
 * @fileOverview aq2rdb, USGS-variant RDB utility functions.
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
            if (begdtm.substring(0, 4) === "0000")
                begdtm = "00000000000000";
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
    }, // fillEndDtm

    /**
       @function Node.js emulation of legacy NWIS, NWF_RDB_OUT()
                 Fortran subroutine: "Top-level routine for outputting
                 rdb format data".
       @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
       @author <a href="mailto:sbarthol@usgs.gov">Scott Bartholoma</a>
       @param {string} intyp rating (time series?) type
       @param {Boolean} inrndsup 
       @param {Boolean} inwyflag
       @param {Boolean} incflag
       @param {Boolean} invflag
       @param {string} inagny
       @param {string} instnid
       @param {string} inddid
       @param {string} inlocnu
       @param {string} instat
       @param {string} intrans
       @param {string} begdat
       @param {string} enddat,
       @param {string} inLocTzCd
       @param {string} titlline
    */
    out: function (
        intyp, inrndsup, inwyflag, incflag, invflag, inagny, instnid,
        inddid, inlocnu, instat, intrans, begdat, enddat,
        inLocTzCd, titlline
    ) {
        // init control argument
        var sopt = "10000000000000000000000000000000".split("");
        var datatyp, rtagny, needstrt, sid, stat;

        if (intyp.length > 2)
            datatyp = intyp.substring(0, 2);
        else
            datatyp = intyp;

        datatyp = datatyp.toUpperCase(); // CALL s_upcase (datatyp,2)

        // convert agency to 5 characters - default to USGS
        if (inagny === undefined)
            rtagny = "USGS";
        else {
            if (inagny.length > 5)
                rtagny = inagny.substring(0, 5);
            else
                rtagny = inagny;
            rtagny = sprintf("%-5s", rtagny); // CALL s_jstrlf (rtagny,5)
        }

        // convert station to 15 characters
        if (instnid === undefined)
            needstrt = true;
        else {
            if (instnid.length > 15)            
                sid = instnid.substring(0, 15);
            else
                sid = instnid;
            sid = sprintf("%-15s", sid); // CALL s_jstrlf (sid, 15)
        }

        // DDID is only needed IF parm and loc number are not
        // specified
        if (inddid === undefined) {
            needstrt = true;
            sopt[4] = '2';
        }

        // further processing depends on data type

        if (datatyp === 'DV') { // convert stat to 5 characters
            if (instat === undefined) {
                needstrt = true;
                sopt[7] = '1';
            }
            else {
                if (5 < instat.length)
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
                begdate = fillBegDate(wyflag, begdat);
                enddate = fillEndDate(wyflag, enddat);
            }

        }

        if (datatyp === 'UV') {

            uvtyp = instat.charAt(0);
            // TODO: this residue of legacy code below can obviously
            // be condensed
            if (uvtyp === 'm') uvtyp = 'M';
            if (uvtyp === 'n') uvtyp = 'N';
            if (uvtyp === 'e') uvtyp = 'E';
            if (uvtyp === 'r') uvtyp = 'R';
            if (uvtyp === 's') uvtyp = 'S';
            if (uvtyp === 'c') uvtyp = 'C';
            if (uvtyp !== 'M' .AND. uvtyp !== 'N' .AND. 
                uvtyp !== 'E' .AND. uvtyp !== 'R' .AND. 
                uvtyp !== 'S' .AND. uvtyp !== 'C') {
                // TODO: this is a prompt loop in legacy code;
                // raise error here?
                // 'Please answer "M", "N", "E", "R", "S", or "C".',
            }

            // convert date/times to 14 characters
            if (begdat === undefined || enddat === undefined) {
                needstrt = true;
                if (wyflag)
                    sopt[8] = '4';
                else
                    sopt[9] = '3';
            }
            else {
                begdtm = fillBegDtm(wyflag, begdat);
                enddtm = fillEndDtm(wyflag, enddat);
            }

        }

        // TODO: this conditional might not be needed eventually
        if (needstrt) { // call s_strt if needed
            // call start routine
            prgid = "aq2rdb";
            if (titlline  === undefined) {
                prgdes = "TIME-SERIES TO RDB OUTPUT";
            }
            else {
                if (80 < titlline.length)
                    prgdes = titlline.substring(0, 80);
                else
                    prgdes = titlline;
            }
            rdonly = 1;
            //123         s_strt (sopt, *998)
            sopt[0] = '2';
            rtdbnum = dbnum;    // get DB number first

            if (sopt.charAt(4) === '1' || sopt.charAt(4) === '2') {
                rtagny = agency;        // get agency
                sid = stnid;    // get stn ID
                if (sopt.charAt(4) === '2')
                    ddid = usddid; // and DD number
            }

            // stat code
            if (sopt.charAt(7) === '1') stat = statcd;

            // data type
            if (sopt.charAt(11) === '2') {
                uvtyp_prompted = true;
                if (usdtyp === 'D') {
                    datatyp = "DV";
                    cflag = false;
                }
                else if (usdtyp === 'V') {
                    datatyp = "DV";
                    cflag = true;
                }
                else if (usdtyp === 'U') {
                    datatyp = "UV";
                    uvtyp = 'M';
                }
                else if (usdtyp === 'N') {
                    datatyp = "UV";
                    uvtyp = 'N';
                }
                else if (usdtyp === 'E') {
                    datatyp = "UV";
                    uvtyp = 'E';
                }
                else if (usdtyp === 'R') {
                    datatyp = "UV";
                    uvtyp = 'R';
                }
                else if (usdtyp === 'S') {
                    datatyp = "UV";
                    uvtyp = 'S';
                }
                else if (usdtyp === 'C') {
                    datatyp = "UV";
                    uvtyp = 'C';
                }
                else if (usdtyp === 'M') {
                    datatyp = "MS";
                }
                else if (usdtyp === 'X') {
                    datatyp = "VT";
                }
                else if (usdtyp === 'L') {
                    datatyp = "WL";
                }
                else if (usdtyp === 'Q') {
                    datatyp = "QW";
                }
            }

            // date range for water years
            if (sopt.charAt(8) === '4') {
                if (usyear === "9999") {
                    begdtm = "00000000000000";
                    begdate = "00000000";
                }
                else {
                    usdate = sprintf("%4d1001", parseInt(usyear) - 1);
                    begdtm = usdate + "000000";
                    begdate = usdate;
                }
                if (ueyear === "9999") {
                    enddtm = "99999999999999";
                    enddate = "99999999";
                }
                else {
                    enddtm = sprintf("%4s0930235959", ueyear);
                    enddate = sprintf("%4s0930", ueyear);
                }
            }

            // date range
            if (sopt.charAt(9) === '3') {
                begdate = usdate;
                enddate = uedate;
                begdtm = usdate + "000000";
                if (uedate === "99999999")
                    enddtm = "99999999999999";
                else
                    enddtm = uedate + "235959";
            }
        }
        else {
            // get PRIMARY DD that goes with parm if parm supplied
            if (parm !== undefined && datatyp !== "VT") {
                nwf_get_prdd(rtdbnum, rtagny, sid, parm, ddid, irc);
                if (irc !== 0) {
                    //        WRITE (0,2120) rtagny, sid, parm
                    //2120    FORMAT (/,"No PRIMARY DD for station "",A5,A15,
                    //                "", parm "',A5,'".  Aborting.",/)
                    return irc;
                }
            }
        }

        // retrieving measured uvs and transport_cd not supplied,
        // prompt for it
        if (uvtyp_prompted && datatyp === "UV" &&
            (uvtyp === "M' || uvtyp === 'N") &&
            transport_cd === undefined) {
            /*
              nw_query_meas_uv_type(rtagny, sid, ddid, begdtm,
              enddtm, loc_tz_cd, transport_cd,
              sensor_type_id, *998)
              if (transport_cd === undefined) {
              WRITE (0,2150) rtagny, sid, ddid
              2150      FORMAT (/,"No MEASURED UV data for station "",A5,A15,
              "", DD "',A4,'".  Aborting.",/)
              return irc;
              END IF
            */
        }

        //  get data and output to files

        if (datatyp === "DV") {
            irc = fdvrdbout(funit, false, rndsup, addkey, vflag,
                            cflag, rtagny, sid, ddid, stat, begdate,
                            enddate);
        }
        else if (datatyp === "UV") {
            // TODO: replace legacy residue below with indexed array?
            if (uvtyp === 'M') inguvtyp = "meas";
            if (uvtyp === 'N') inguvtyp = "msar";
            if (uvtyp === 'E') inguvtyp = "edit";
            if (uvtyp === 'R') inguvtyp = "corr";
            if (uvtyp === 'S') inguvtyp = "shift";
            if (uvtyp === 'C') inguvtyp = "da";

            irc = fuvrdbout(funit, false, rtdbnum, rndsup, cflag,
                            vflag, addkey, rtagny, sid, ddid, inguvtyp, 
                            sensor_type_id, transport_cd, begdtm, 
                            enddtm, loc_tz_cd);
        }

        /*
        //  close files and exit
        //997   s_mclos
        s_sclose (funit, "keep")
        nw_disconnect
        return irc;

        //  bad return (do a generic error message)
        //998   irc = 3
        nw_error_handler (irc,"nwf_rdb_out","error",
        "doing something","something bad happened")
        */

        // Good return
        //999
        return irc;

    } // out

} // rdb
