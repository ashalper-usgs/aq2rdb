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
   @param {string} inddid Data descriptor (DD) number.
   @param {string} stat Statistic code.
   @param {string} begdate Begin date.
   @param {string} enddate End date.
   @callback
*/

'use strict';

var async = require('async');

function fdvrdbout(
    response, editable, rndsup, addkey, vflag, compdv, agyin, station,
    inddid, stat, begdate, enddate, callback
) {
    // many/most of these are artifacts of the legacy code, and
    // probably won't be needed:
    var irc, outlen, dvabort, ldv_name, ldv_data_name;
    var ldv_diff_name, rowcount, dv_water_yr, dtlen, tunit;
    var dv_name, dv_data_name, dv_diff_name, cdd_id;
    var ddlabl = ' ', cval, cdate, odate, outlin, rndary = ' ';
    var rndparm, rnddd, cdvabort = ' ', bnwisdt, enwisdt;
    var bnwisdtm, enwisdtm, bingdt, eingdt, temppath;
    var nullval = '**NULL**', nullrd = ' ', nullrmk = ' ';
    var nulltype = ' ', nullaging = ' ';

    var exdate;               // verbose Excel style date "mm/dd/yyyy"
    var dtcolw;               // holds DATE column width '19D' etc.

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
        requestSite,
        receiveSite,
        function (site, callback) {
            if (irc !== 0) {
                sagncy = agyin;
                sid = station;
                sname = '*** NOT IN SITE FILE ***';
                smgtof = ' ';
                slstfl = ' ';
            }

            if (smgtof === ' ') {
                // TODO:
                // WRITE (smgtof,'(I3)') gmtof
            }
            s_jstrlf(smgtof, 3);

            if (slstfl === ' ') {
                if (lstfg === 0) {
                    slstfl = true;
                }
                else {
                    slstfl = false;
                }
            }

            callback(null);
        },
        function (callback) {
            // get DD
            ddagny = agyin;
            ddstid = station;
            ddid = inddid;
            // TODO:
            // s_mddd(nw_read, irc, *998);

            callback(null);
        },
        function (callback) {
            if (irc === 0) {
                s_lbdd(nw_left, ddlabl); // set label
                pcode = 'P';             // pmcode            // set rounding
                pmretr(60, rtcode);
                if (rnddd !== ' ' && rnddd !== '0000000000') {
                    rndary = rnddd;
                }
                else {
                    rndary = rndparm;
                }
            }
            else {
                ddagny = agyin;
                ddstid = station;
                ddid = inddid;
                ddlabl = '*** NOT IN DD FILE ***';
                rndary = '9999999992';
            }

            callback(null);
        },
        // TODO: this might be obsolete
        function (callback) {
            //  DV abort limit defaults to 120 minutes
            cdvabort = '120';

            //  get the DV abort limit
            if (dbRetrDVAbort(ddagny, ddstid, ddid, bnwisdtm,
                              enwisdtm, dvabort)) {
                // TODO:
                // WRITE(cdvabort,'(I6)') dvabort;
                s_jstrlf(cdvabort, 6);
            }

            callback(null);
        },
        function (callback) {
            // get stat information
            s_statck(stat, irc);
            if (irc !== 0) {
                ssnam = '*** INVALID STAT ***';
            }

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
                        if (editable) {
                            funit.write(
                                '# //FILE TYPE="NWIS-I DAILY-VALUES" ' +
                                    'EDITABLE=YES\n'
                            );
                        }
                        else {
                            funit.write(
                                '# //FILE TYPE="NWIS-I DAILY-VALUES" ' +
                                    'EDITABLE=NO\n'
                            );
                        }

                        callback(null);
                    },
                    function (callback) {
                        // write database info
                        rdbDBLine(funit), // <- TODO
                        callback(null);
                    },
                    function (callback) {
                        // write site info
                        funit.write(
                            '# //STATION AGENCY="' + sagncy +
                                '" NUMBER="' + sid + '" TIME_ZONE="' +
                                smgtof + '" DST_FLAG=' + slstfl + '\n' +
                                '# //STATION NAME="' + sname + '"\n'
                        );
                        callback(null);
                    },
                    function (callback) {
                        // write Location info
                        // TODO:
                        rdbWriteLocInfo(funit, dd_id);
                        callback(null);
                    },
                    function (callback) {
                        // write DD info
                        funit.write(
                            '# //DD DDID="' + ddid + '" RNDARY="' +
                                rndary + '" DVABORT=' + cdvabort + '\n' +
                                '# //DD LABEL="' + ddlabl + '"\n' +
                                '# //PARAMETER CODE="' +
                                pcode.substr(1, 5) + '" SNAME = "' +
                                psnam + '"\n' +
                                '# //PARAMETER LNAME="' + plname +
                                '"\n' +
                                '# //STATISTIC CODE="' +
                                scode.substr(1, 5) + '" SNAME="' +
                                ssnam + '"\n' +
                                '# //STATISTIC LNAME="' + slname + '"\n'
                        );
                        callback(null);
                    },
                    function (callback) {
                        // write DV type info
                        if (compdv) {
                            funit.write(
                                '# //TYPE NAME="COMPUTED" ' +
                                    'DESC = "COMPUTED DAILY VALUES ONLY"\n'
                            )
                        }
                        else {
                            funit.write(
                                '# //TYPE NAME="FINAL" ' +
                                   'DESC = "EDITED AND COMPUTED DAILY VALUES"\n'
                            )
                        }
                        callback(null);
                    },
                    function (callback) {
                        // write data aging information
                        // TODO:
                        rdbWriteAging(
                            funit, dbnum, dd_id, begdate, enddate
                        );
                        callback(null);
                    },
                    function (callback) {
                        // write editable range
                        funit.write(
                            '# //RANGE START="' + begdate +
                                '" END="' + enddate + '"\n'
                        )
                        callback(null);
                    },
                    function (callback) {
                        callback(null);
                    },
                    function (callback) {
                        callback(null);
                    },
                    function (callback) {
                        callback(null);
                    },
                    function (callback) {
                        callback(null);
                    },
                    function (callback) {
                        callback(null);
                    },
                    function (callback) {
                        callback(null);
                    },
                    function (callback) {
                        callback(null);
                    },
                    function (callback) {
                        callback(null);
                    }
                ]);

                // write single site RDB column headings
                outlin =
                    'DATE\tTIME\tVALUE\tPRECISION\tREMARK\tFLAGS\tTYPE\tQA\n';

                // TODO:
                // WRITE (funit,'(20A)') outlin(1:46)

                if (vflag) {            // verbose Excel-style format
                    dtcolw = '10D';     // "mm/dd/yyyy" 10 chars
                    dtlen = 3;
                }
                else {
                    dtcolw = '8D';      // "yyyymmdd" 8 chars
                    dtlen = 2;
                }
                outlin = dtcolw.substr(0, dtlen - 1) +
                    '\t6S\t16N\t1S\t1S\t32S\t1S\t1S';
                // TODO:
                /*
                  WRITE (funit,'(20A)') outlin(1:23+dtlen)
                */
            }
            else {
                if (first) {

                    // write "with keys" rdb column headings
                    // TODO:
                    /*
                      WRITE (funit,'(20A)')
                      *           '# //FILE TYPE="NWIS-I DAILY-VALUES" ',
                      *           'EDITABLE=NO'
                      */

                    // write database info
                    nw_rdb_dbline(funit);

                    outlin =
                        'AGENCY\tSTATION\tDD\tPARAMETER\tSTATISTIC\tDATE\t' +
                        'TIME\tVALUE\tPRECISION\tREMARK\tFLAGS\tTYPE\tQA\n'
                    // TODO:
                    /*
                      WRITE (funit,'(20A)') outlin(1:84)
                    */

                    if (vflag) {        // verbose Excel-style format
                        dtcolw = '10D';  // "mm/dd/yyyy" 10 chars
                        dtlen = 3;
                    }
                    else {
                        dtcolw = '8D';   // "yyyymmdd" 8 chars
                        dtlen = 2;
                    }
                    outlin = '5S\t15S\t4S\t5S\t5S\t' + dtcolw +
                        '\t6S\t16N\t1S\t1S\t32S\t1S\t1S\n';
                    // TODO:
                    /*
                      WRITE (funit,'(20A)') outlin(1:39+dtlen)
                    */
                    first = false;
                }
            }

            callback(null);
        }
    ]);

    // If the DD does not exist, no sense in trying to retrieve any data
    if (ddlabl !== '*** NOT IN DD FILE ***') {

        // get DV table names
        rtcode = 0;
        if (! nw_getchk_table_nm ('DV', dbnum, 'R', dv_name, ldv_name)) {
            rtcode = 4;
            return;
        }
        if (! nw_getchk_table_nm('DV_DATA', dbnum, 'R', dv_data_name,
                                 ldv_data_name)) {
            rtcode = 4;
            return;
        }
        if (compdv) {
            if (! nw_getchk_table_nm('DV_DIFF', dbnum, 'R',
                                     dv_diff_name, ldv_diff_name)) {
               rtcode = 4;
               return;
            }
        }

        // Setup begin date
        if (begdate === '00000000') {
            if (! nw_db_retr_dv_first_yr(dd_id, stat, dv_water_yr)) {
                rtcode = nw_get_error_number();
                return;
            }
            // TODO:
            /*
            WRITE (bnwisdt,2030) dv_water_yr - 1
 2030       FORMAT (I4.4,'1001')
 */
        }

        // validate and load begin date into ingres FORMAT
        if (! nw_cdt_ok(bnwisdt)) {
            rtcode = 3;
            return;
        }
        else {
            nw_dt_nwis2ing(bnwisdt, bingdt)
        }

        // Setup end date
        if (enddate === '99999999') {
            if (! nw_db_retr_dv_last_yr(dd_id, stat, dv_water_yr)) {
                rtcode = nw_get_error_number();
                return;
            }
            // TODO:
            /*
            WRITE (enwisdt,2040) dv_water_yr
 2040       FORMAT (I4.4,'0930')
 */
         }

        // validate and load end date into ingres FORMAT
        if (! nw_cdt_ok(enwisdt)) {
            rtcode = 3;
            return;
        }
        else {
            nw_dt_nwis2ing(enwisdt, eingdt);
        }

        // Get DV data to a temporary file
        nwc_tmpnam(temppath)
        getfunit(1, tunit)
        // TODO:
        /*
        OPEN (UNIT=tunit, FILE=temppath,
     *        FORM='unformatted', STATUS='unknown')
         REWIND (tunit)
         */

        nwc_itoa(dd_id, cdd_id, 12);
        odate = bnwisdt;
        if (! compdv) {
            // TODO:
            /*
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
            // TODO:
            /*
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
            if (dv_rd === ' ') {
                if (! nw_va_rget_rd(dv_va, rndary, dv_rd)) {
                    dv_rd = '9';
                }
            }
                     
            // convert value to a character string and load it
            if (rndsup) {
                // TODO:
                /*
                        WRITE (cval,2050) dv_va
 2050                   FORMAT (E14.7)
 */
            }
            else {
                if (! nw_va_rrnd_tx(dv_va, dv_rd, cval)) {
                    cval = '****';
                }
            }
            s_jstrlf(cval, 20);
            // TODO:
            /*
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
        if (irc !== 0) {
            rtcode = irc;
            return;
        }
         
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

         if (vflag) {           // build "mm/dd/yyyy"
             exdate = cdate.substr(4, 5) + '/' + cdate.substr(6, 7) +
                 '/' + cdate.substr(0, 3);
         }
        else {                  // copy over "yyyymmdd"
            exdate = cdate;
        }

        if (addkey) {
            outlin = agyin + '\t' + station + '\t' + inddid + '\t' +
                pcode.substr(1, 5) + '\t' + stat + '\t' + exdate;
         }
        else {
            outlin = exdate;
        }
        outlen = nwf_strlen(outlin);

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
        outlen = nwf_strlen(outlin);
        // TODO:
        /*
        WRITE (funit,'(20A)') outlin(1:outlen)
         GOTO 30

 40      CLOSE (tunit, status = 'DELETE')
         */
      }

      if (! wrotedata && rtcode !== 1)
          rtcode = 2;

    return rtcode;

    // TODO:
    /*
998   s_fatal ('Fatal DD file error', 'fdvrdbout', 'M', irc)

999   s_fatal ('Fatal DV file error', 'fdvrdbout', 'M', irc)
*/

    // RTCODE  =  0 - all output OK
    //   1 - some or all of data marked final
    //   2 - No non-null data in retrieval
    //   3 - error - bad begin or end date - no file written

    callback(null);
} // fdvrdbout
