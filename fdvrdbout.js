/**
 * @fileOverview A Node.js emulation of legacy NWIS, FDVRDBOUT()
 *               Fortran subroutine.
 *
 * @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
 *
 */

'use strict';

/**
   @description Node.js modules.
*/
var async = require('async');
var sprintf = require("sprintf-js").sprintf;

/**
   @description aq2rdb modules.
*/
var site = require('./site');

/**
   @description Emulate legacy NWIS, FDVRDBOUT() Fortran subroutine:
                "Write DV data in rdb FORMAT" [sic].
   @author <a href="mailto:ashalper@usgs.gov">Andrew Halper</a>
   @author <a href="mailto:sbarthol@usgs.gov">Scott D. Bartholoma</a>
   @param {object} response IncomingMessage object created by http.Server.
   @param {Boolean} editable true if DVs are editable; false otherwise.
   @param {Boolean} rndsup true to suppress rounding; false otherwise.
   @param {Boolean} addkey true if logical key is to be added to each row.
   @param {Boolean} vflag true to produce Excel-style, verbose dates/times.
   @param {Boolean} compdv true to retrieve computed DVs only.
   @param {string} agyin Site agency code.
   @param {string} station Site number (a.k.a. site ID).
   @param {string} stat Statistic code.
   @param {string} begdate Begin date.
   @param {string} enddate End date.
   @callback
*/
function fdvrdbout(
    token, editable, rndsup, addkey, vflag, compdv, agyin, station,
    stat, begdate, enddate, response, callback
) {
    // many/most of these are artifacts of the legacy code, and
    // probably won't be needed:
    var irc, dvabort;
    var dv_water_yr, tunit;
    var cval, cdate, odate, outlin, rndary = ' ';
    var rndparm, rnddd, cdvabort = ' ', bnwisdt, enwisdt;
    var bnwisdtm, enwisdtm, bingdt, eingdt, temppath;
    var nullval = '**NULL**', nullrd = ' ', nullrmk = ' ';
    var nulltype = ' ', nullaging = ' ';
    var smgtof, rtcode;
    var wrotedata = false, first = true;

    async.waterfall([
        function (callback) {
            if (begdate === '00000000') {
                // RH comes from NW_NWIS_MINDATE value in
                // watstore/support/ins.dir/adaps_keys.ins
                bnwisdtm = '15820101235959';
                bnwisdt = bnwisdtm.substr(0, 7);
            }
            else {
                bnwisdtm = begdate + '000000';
                bnwisdt = begdate;
            }

            if (enddate === '99999999') {
                // RH comes from NW_NWIS_MAXDATE value in
                // watstore/support/ins.dir/adaps_keys.ins
                enwisdtm = '23821231000000'
                enwisdt = enwisdtm.substr(0, 7);
            }
            else {
                enwisdt = enddate;
                enwisdtm = enddate + '23959';
            }

            // get site
            sagncy = agyin;
            sid = station;

            callback(null, sid);
        },
        site.request,
        /**
           @function Receive and parse response from USGS Site Web Service.
           @callback
           @param {string} messageBody Message body of HTTP response from USGS
                                       Site Web Service.
           @param {function} callback Callback to call when complete.
        */
        function(messageBody, callback) {
            var site = new Object;

            /**
               @todo Here we're parsing RDB, which is messy, and would
                     be nice to encapsulate.
            */
            try {
                // parse (station_nm,tz_cd,local_time_fg) from RDB
                // response
                var row = messageBody.split('\n');
                // RDB column names
                var columnName = row[row.length - 4].split('\t');
                // site column values are in last row of table
                var siteField = row[row.length - 2].split('\t');

                // the necessary site fields
                sagncy = siteField[columnName.indexOf('agency_cd')];
                sid = siteField[columnName.indexOf('site_no')];
                sname = siteField[columnName.indexOf('station_nm')];
                /**
                   @todo nwts2rdb appeared to reference the time zone
                         offset instead of the code (see smgtof
                         processing below)? Need to figure out if this
                         is still relevant.
                 */
                site.tzCode = siteField[columnName.indexOf('tz_cd')];
                slstfl = siteField[columnName.indexOf('local_time_fg')];
            }
            catch (error) {
                /**
                   @todo Need to research the USGS Site Web Service
                         semantics for "site not found" assertion, and
                         emulate this legacy code accordingly.

                if (irc !== 0) {
                    sagncy = agyin;
                    sid = station;
                    sname = '*** NOT IN SITE FILE ***';
                    smgtof = ' ';
                    slstfl = ' ';
                }
                 */
                callback(error);
                return;
            }
            callback(null, site);
        },
        function (site, callback) {
            if (smgtof === ' ') {
                smgtof = sprintf("%3d", gmtof); // WRITE (smgtof,'(I3)') gmtof
            }
            smgtof = sprintf("%-3d", smgtof);
            callback(null);
        },
        function (callback) {
            pcode = 'P';             // pmcode            // set rounding
            /**
               @todo Load data descriptor?
               s_mddd(nw_read, irc, *998);
            */
            /**
               @todo call parameter Web service here
               rtcode = pmretr(60);
            */
            if (rnddd !== ' ' && rnddd !== '0000000000')
                rndary = rnddd;
            else
                rndary = rndparm;

            callback(null);
        },
        /**
           @todo this might be obsolete
         */
        function (callback) {
            // DV abort limit defaults to 120 minutes
            cdvabort = '120';

            /**
               @todo get the DV abort limit
	       @see watstore/adaps/adrsrc/tsing_lib/nw_db_retr_dvabort.sf
               if (dbRetrDVAbort(ddagny, ddstid, ddid, bnwisdtm,
                                 enwisdtm, dvabort)) {
	          cdvabort = sprintf("%6d", dvabort);
	       }
            */
            callback(null);
        },
        function (callback) {
            /**
               @todo get stat information
               irc = s_statck(stat);
            */
            if (irc !== 0)
                ssnam = '*** INVALID STAT ***';

            callback(null);
        },
        function (callback) {
            if (! addkey) {
                async.waterfall([
                    function (callback) {
                        // write the header records
                        rdbHeader(funit);
                        callback(null);
                    },
                    function (callback) {
                        var line =
                            '# //FILE TYPE="NWIS-I DAILY-VALUES" EDITABLE=';
                        
                        if (editable)
                            line += "YES";
                        else
                            line += "NO";

                        funit.write(line + '\n', "ascii");
                        callback(null);
                    },
                    function (callback) {
                        /**
                           @todo write database info
                         */
                        rdbDBLine(funit);
                        callback(null);
                    },
                    function (callback) {
                        // write site info
                        funit.write(
                            '# //STATION AGENCY="' + sagncy +
                                '" NUMBER="' + sid + '" TIME_ZONE="' +
                                smgtof + '" DST_FLAG=' + slstfl + '\n' +
                                '# //STATION NAME="' + sname + '"\n',
                            "ascii"
                        );
                        callback(null);
                    },
                    function (callback) {
                        /**
                           @todo write Location info

                           At 8:30 AM, Feb 16th, 2016, Wade Walker
                           <walker@usgs.gov> said:

                           sublocation is the AQUARIUS equivalent of
                           ADAPS location. It is returned from any of
                           the GetTimeSeriesDescriptionList... methods
                           or for GetFieldVisitData method elements
                           where sublocation is
                           appropriate. GetSensorsAndGages will also
                           return associated sublocations. They're
                           basically just a shared attribute of time
                           series, sensors and gages, and field
                           readings, so no specific call for them,
                           they're just returned with the data they're
                           applicable to. Let me know if you need
                           something beyond that.

                           rdbWriteLocInfo(funit, dd_id);
                         */
                        callback(null);
                    },
                    function (callback) {
                        // write DD info
                        funit.write(
                                '# //PARAMETER CODE="' +
                                pcode.substr(1, 5) + '" SNAME = "' +
                                psnam + '"\n' +
                                '# //PARAMETER LNAME="' + plname +
                                '"\n' +
                                '# //STATISTIC CODE="' +
                                scode.substr(1, 5) + '" SNAME="' +
                                ssnam + '"\n' +
                                '# //STATISTIC LNAME="' + slname + '"\n',
                            "ascii"
                        );
                        callback(null);
                    },
                    function (callback) {
                        // write DV type info
                        if (compdv) {
                            funit.write(
                                '# //TYPE NAME="COMPUTED" ' +
                                    'DESC = "COMPUTED DAILY VALUES ONLY"\n',
                                "ascii"
                            )
                        }
                        else {
                            funit.write(
                                '# //TYPE NAME="FINAL" ' +
                                'DESC = "EDITED AND COMPUTED DAILY VALUES"\n',
                                "ascii"
                            )
                        }
                        callback(null);
                    },
                    function (callback) {
                        /**
                           @todo write data aging information
                           rdbWriteAging(
                           funit, dbnum, dd_id, begdate, enddate
                           );
                         */
                        callback(null);
                    },
                    function (callback) {
                        // write editable range
                        funit.write(
                            '# //RANGE START="' + begdate +
                                '" END="' + enddate + '"\n',
                            "ascii"
                        )
                        callback(null);
                    },
                    function (callback) {
                        // write single site RDB column headings
                        funit.write(
                            "DATE\tTIME\tVALUE\tPRECISION\t" +
                                "REMARK\tFLAGS\tTYPE\tQA\n",
                            "ascii"
                        );
                        callback(null);
                    },
                    function (callback) {
                        var dtcolw;
                        
                        // if verbose, Excel-style format
                        if (vflag) {
                            dtcolw = '10D';     // "mm/dd/yyyy" 10 chars
                        }
                        else {
                            dtcolw = '8D';      // "yyyymmdd" 8 chars
                        }

                        // WRITE (funit,'(20A)') outlin(1:23+dtlen)
                        funit.write(
                            dtcolw + "\t6S\t16N\t1S\t1S\t32S\t1S\t1S",
                            "ascii"
                        );
                        callback(null);
                    }
                ]); // async.waterfall
            }
            else if (first) {
                async.waterfall([
                    function (callback) {
                /**
                   @todo write "with keys" rdb column headings
                   WRITE (funit,'(20A)')
                   *           '# //FILE TYPE="NWIS-I DAILY-VALUES" ',
                   *           'EDITABLE=NO'
                   */
                        callback(null);
                    },
                    function (callback) {
                        // write database info
                        nw_rdb_dbline(funit);
                        callback(null);
                    },
                    function (callback) {
                        funit.write(
                           "AGENCY\tSTATION\tDD\tPARAMETER\tSTATISTIC\tDATE\t" +
                           "TIME\tVALUE\tPRECISION\tREMARK\tFLAGS\tTYPE\tQA\n",
                           "ascii"
                        );
                        callback(null);
                    },
                    function (callback) {
                        var dtcolw;
                        
                        // if verbose, Excel-style format
                        if (vflag) {
                            dtcolw = "10D";  // "mm/dd/yyyy" 10 chars
                        }
                        else {
                            dtcolw = "8D";   // "yyyymmdd" 8 chars
                        }

                        funit.write(
                            "5S\t15S\t4S\t5S\t5S\t" + dtcolw +
                                "\t6S\t16N\t1S\t1S\t32S\t1S\t1S\n",
                            "ascii"
                        );
                        
                        first = false;
                        callback(null);
                    }
                ]);
            }
            callback(null);
        },
        function (callback) {
            // Setup begin date
            if (begdate === '00000000') {
                if (! nw_db_retr_dv_first_yr(dd_id, stat, dv_water_yr)) {
                    return nw_get_error_number();
                }
                /**
                   WRITE (bnwisdt,2030) dv_water_yr - 1
                   2030       FORMAT (I4.4,'1001')
                */
                bnwisdt = sprintf("%4d1001", dv_water_yr - 1);
            }

            // validate and load begin date into ingres FORMAT
            if (! nw_cdt_ok(bnwisdt))
                return 3;
            else
                nw_dt_nwis2ing(bnwisdt, bingdt)

            // Setup end date
            if (enddate === '99999999') {
                if (! nw_db_retr_dv_last_yr(dd_id, stat, dv_water_yr)) {
                    return nw_get_error_number();
                }
                /*
                  WRITE (enwisdt,2040) dv_water_yr
                  2040       FORMAT (I4.4,'0930')
                */
                enwisdt = sprintf("%4d0930", dv_water_yr);
            }

            // validate and load end date into ingres FORMAT
            if (! nw_cdt_ok(enwisdt))
                return 3;
            else
                nw_dt_nwis2ing(enwisdt, eingdt);

            odate = bnwisdt;
            if (! compdv) {
                /**
                   @todo

                   stmt = 'SELECT dvd.dv_dt, dvd.dv_va, dvd.dv_rd, ' //
                   *                    'dvd.dv_rmk_cd, dvd.dv_type_cd, ' //
                   *                    'dvd.data_aging_cd FROM ' //
                   *                dv_data_name(1:ldv_data_name) // ' dvd, ' //
                   *                dv_name(1:ldv_name) // ' dv ' //
                   *             'WHERE dv.dd_id = ' // cdd_id // ' AND ' //
                   *                   'dv.stat_cd = ''' // stat // ''' AND ' //
                   *                   'dvd.dv_id = dv.dv_id AND ' // 
                   *                   'dvd.dv_dt >=  ''' // bingdt // ''' AND ' // 
                   *                   'dvd.dv_dt <=  ''' // eingdt // ''' '  // 
                   *             'ORDER BY dvd.dv_dt'
                   */
            }
            else {
                /*
                  @todo

                  stmt = 'SELECT dvd.dv_dt, dvd.dv_va, dvd.dv_rd,' //
                  *               'dvd.dv_rmk_cd, dvd.dv_type_cd,' //
                  *               'dvd.data_aging_cd FROM ' //
                  *             dv_data_name(1:ldv_data_name) // ' dvd, ' //
                  *             dv_name(1:ldv_name) // ' dv ' //
                  *             'WHERE dv.dd_id = ' // cdd_id // ' AND ' //
                  *             'dv.stat_cd = ''' // stat // ''' AND  ' // 
                  *             'dvd.dv_id = dv.dv_id AND ' //
                  *             'dvd.dv_type_cd = ''C'' AND ' //
                  *             'dvd.dv_dt >=  ''' // bingdt // ''' AND ' //
                  *             'dvd.dv_dt <=  ''' // eingdt // ''' ' // 
                  *             ' UNION ' //
                  *             'SELECT dvf.dv_dt, dvf.dv_va, dvf.dv_rd, ' //
                  *               'dvf.dv_rmk_cd, dvf.dv_type_cd, ' //
                  *               'dvf.data_aging_cd FROM ' //
                  *             dv_diff_name(1:ldv_diff_name) // ' dvf, ' //
                  *             dv_name(1:ldv_name) // ' dv ' //
                  *             'WHERE dv.dd_id = ' // cdd_id // ' AND ' //
                  *             'dv.stat_cd = ''' // stat // ''' AND ' // 
                  *             'dvf.dv_id = dv.dv_id AND ' //
                  *             'dvf.dv_type_cd = ''C'' AND ' //
                  *             'dvf.dv_dt >=  ''' // bingdt // ''' AND ' //
                  *             'dvf.dv_dt <=  ''' // eingdt // ''' ' //
                  *             ' ORDER BY dv_dt'
                  */
            }

            // TODO:
            /*
              EXEC SQL PREPARE pstmt FROM :stmt
              nw_sql_error_handler ('fdvrdbout', 'prepare',
              *        'Retrieving DV data', rowcount, irc)
              if (irc === 0) {
              EXEC SQL OPEN cur_stmt
              nw_sql_error_handler ('fdvrdbout', 'opencurs',
              *           'Retrieving DV data', rowcount, irc)
              if (irc === 0) {
              DO
              EXEC SQL FETCH cur_stmt INTO
              *                 :dv_dt, :dv_va:dv_va_null, :dv_rd:dv_rd_null,
              *                 :dv_rmk_cd, :dv_type_cd, :data_aging_cd

              if (nw_no_data_found()) EXIT
              
              nw_sql_error_handler ('fdvrdbout', 'fetch',
              *                 'Retrieving DV data', rowcount, irc)
              if (irc !== 0) EXIT

              nw_dt_ing2nwis (dv_dt, cdate)
              10               if (odate .LT. cdate) {
              WRITE (tunit) odate, nullval, nullrd, nullrmk, 
              *                    nulltype, nullaging
              nw_dtinc (odate, 1)
              GO TO 10
              }
            */
            // process this row
            if (dv_va_null === -1) dv_va = NW_NR4;
            if (dv_rd_null === -1) dv_rd = ' ';
            if (dv_va < NW_CR4) {
                // value is null
                // TODO:
                /*
                  WRITE (tunit) cdate, nullval, nullrd, dv_rmk_cd,
                  *                    dv_type_cd, data_aging_cd
                  */
            }
            else {
                // Pick a rounding precision IF blank
                if (dv_rd === ' ')
                    if (! nw_va_rget_rd(dv_va, rndary, dv_rd))
                        dv_rd = '9';
                
                // convert value to a character string and load it
                if (rndsup) {
                    /**
                       @todo
                       WRITE (cval,2050) dv_va
                       2050                   FORMAT (E14.7)
                    */
                }
                else {
                    if (! nw_va_rrnd_tx(dv_va, dv_rd, cval))
                        cval = '****';
                }
                cval = sprintf("%20s", cval);
                /**
                   @todo
                   WRITE (tunit) cdate, cval, dv_rd, dv_rmk_cd,
                   *                    dv_type_cd, data_aging_cd
                   */
            }
            nw_dtinc(odate, 1);
            // TODO:
            /*
              END DO
              EXEC SQL CLOSE cur_stmt
              end if
              end if
            */
            if (irc !== 0)
                return irc;
            
            // fill nulls to the end of the period, if the database
            // retrieval stopped short
            
            while (odate <= enwisdt) {
                // TODO:
                /*
                  WRITE (tunit) odate, nullval, nullrd, nullrmk, nulltype,
                  *           nullaging
                  */
                nw_dtinc(odate, 1);
            }
            // TODO:
            // ENDFILE (tunit)

            // Read the temp file and write the RDB file, filling in data
            // aging where blank (did it this way because the data aging
            // routine would COMMIT the loop

            // TODO:
            /*
              REWIND (tunit)
              30       READ (tunit,END=40) cdate, cval, dv_rd, dv_rmk_cd,
              *           dv_type_cd, data_aging_cd
              */
            if (data_aging_cd === ' ') {
                if (! nw_db_retr_aging_for_date(dbnum, dd_id, cdate,
                                                data_aging_cd))
                    return;
            }
            if (data_aging_cd !== 'W')
                rtcode = 1;

            var exdate;   // verbose Excel style date "mm/dd/yyyy"

            if (vflag) {           // build "mm/dd/yyyy"
                exdate = cdate.substr(4, 5) + '/' + cdate.substr(6, 7) +
                    '/' + cdate.substr(0, 3);
            }
            else {                  // copy over "yyyymmdd"
                exdate = cdate;
            }

            if (addkey) {
                outlin = agyin + '\t' + station + '\t' +
                    pcode.substr(1, 5) + '\t' + stat + '\t' +
                    exdate;
            }
            else {
                outlin = exdate;
            }

            if (cval === '**NULL**') {
                outlin += '\t\t\t\t' + dv_rmk_cd + '\t\t' +
                    dv_type_cd + '\t' + data_aging_cd;
            }
            else {
                outlin += '\t\t' + cval + '\t' + dv_rd + '\t' +
                    dv_rmk_cd + '\t\t' + dv_type_cd + '\t' +
                    data_aging_cd;
                wrotedata = true;
            }

            /**
               WRITE (funit,'(20A)') outlin(1:outlen)
               GOTO 30

               40      CLOSE (tunit, status = 'DELETE')
            */
            funit.write(outlin + '\n', "ascii");
            callback(null);
        }
    ]); // async.waterfall

    if (! wrotedata && rtcode !== 1)
        rtcode = 2;

    // RTCODE  =  0 - all output OK
    //   1 - some or all of data marked final
    //   2 - No non-null data in retrieval
    //   3 - error - bad begin or end date - no file written
    return rtcode;

    /**
      @todo
      998   s_fatal ('Fatal DD file error', 'fdvrdbout', 'M', irc)
      999   s_fatal ('Fatal DV file error', 'fdvrdbout', 'M', irc)
     */
} // fdvrdbout
