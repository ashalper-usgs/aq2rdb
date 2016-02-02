# File - rdb_out.py
#
# Purpose - Python emulation of legacy NWIS NWF_RDB_OUT() Fortran 77
#           subroutine: Top-level routine for outputting RDB format
#           data.
#
# Authors - Andrew Halper <ashalper@usgs.gov> [Python translation]
#           Scott D. Bartholoma <sbarthol@usgs.gov> [NWF_RDB_OUT()]
#

def goto_999():
    #  Good return
    nwf_rdb_out = irc
    sys.exit(0)

def write_2120(agny, sid, parm):
    print "No PRIMARY DD for station \"" + agny + sid + \
        "\", parm \"" + parm + "\".  Aborting."

# returns the error code from modules called (0 IF all OK)
def rdb_out(
        ctlpath,                # control file path/name
        inmultiple,             # Y/N flag to do multiple ratings
        outpath,                # output file path/name
        indbnum,                # database number (obsolete)
        intyp,                  # rating type
        inrndsup,               # Y/N flag for rounding-suppressed
        inwyflag,               # Y/N flag for water-year
        incflag,        # Y/N flag for Computed DVs/Combined Datetimes (UVs)
        invflag,                # Y/N flag for verbose dates and times
        inhydra,                # Y/N flag if handling data for Hydra
        inagny,                 # agency code
        instnid,                # station number
        inddid,                 # DD number
        inlocnu,                # Location number
        instat,                 # Statistics code
        intrans,                # UV Transport code
        begdat,                 # begin date
        enddat,                 # end date
        in_loc_tz_cd,           # time zone code
        titlline                # title line (text)
):

    # **********************************************************************
    # * FUNCTION DECLARATIONS
    # **********************************************************************

    #      INTEGER nwf_strlen,
    #     *        nwc_rdb_cfil,
    #     *        nw_get_error_number,
    #     *        nwc_atoi

    #      LOGICAL nw_write_log_entry,
    #     *        nw_key_get_zone_dst,
    #     *        nw_get_dflt_tzcd,
    #     *        nw_db_save_program_info,
    #     *        nw_db_key_get_dd_parm_loc

    # **********************************************************************
    # * EXTERNAL SUBROUTINES OR FUNCTIONS
    # **********************************************************************

    #      EXTERNAL nwf_strlen,
    #     *         nwc_rdb_cfil,
    #     *         nw_write_log_entry,
    #     *         nw_get_error_number,
    #     *         nw_key_get_zone_dst,
    #     *         nw_get_dflt_tzcd,
    #     *         nw_db_save_program_info

    # **********************************************************************
    # * INTRINSIC FUNCTIONS
    # **********************************************************************

    #      INTRINSIC len

    # **********************************************************************
    # * INCLUDE FILES
    # **********************************************************************

    #      INCLUDE 'program_id.ins'
    #      INCLUDE 'adaps_keys.ins'
    #      INCLUDE 'user_data.ins'
    #      INCLUDE 'ins.dbdata'

    # **********************************************************************
    # * LOCAL VARIABLE DECLARATIONS
    # **********************************************************************

    #      CHARACTER datatyp*2,
    #     *          savetyp*2,
    #     *          sopt*32,
    #     *          ctlfile*128,
    #     *          rdbfile*128,
    #     *          rtagny*5,
    #     *          sid*15,
    #     *          ddid*6,
    #     *          lddid*6,
    #     *          parm*5,
    #     *          stat*5,
    #     *          transport_cd*1,
    #     *          uvtyp*1,
    #     *          inguvtyp*6,
    #     *          mstyp*1,
    #     *          mssav*1,
    #     *          vttyp*1,
    #     *          wltyp*1,
    #     *          meth_cd*5,
    #     *          pktyp*1,
    #     *          qwparm*5,
    #     *          qwmeth*5,
    #     *          begdate*8,
    #     *          enddate*8,
    #     *          begdtm*14,
    #     *          enddtm*14,
    #     *          bctdtm*14,
    #     *          ectdtm*14,
    #     *          cdate*8,
    #     *          ctime*6,
    #     *          tz_cd*6,
    #     *          loc_tz_cd*6,
    #     *          local_time_fg*1

#      INTEGER rtdbnum,
    #     *        loc_nu,
    #     *        funit,
    #     *        rdblen,
    #     *        ipu,
    #     *        irc,
    #     *        nline,
    #     *        sensor_type_id,
    #     *        one,
    #     *        two,
    #     *        three,
    #     *        iyr,
    #     *        i

#      INTEGER USBUFF(91),
    #     &        HOLDBUFF(91)     ! restored old code from rev 1.5

#      LOGICAL needstrt,
    #     *        multiple,
    #     *        rndsup,
    #     *        wyflag,
    #     *        cflag,
    #     *        vflag,
    #     *        hydra,
    #     *        first,
    #     *        addkey,
    #     *        uvtyp_prompted

# **********************************************************************
# * INITIALIZATIONS
    # **********************************************************************

    one, two, three = 1, 2, 3

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    #     initialize

    needstrt = False
    uvtyp_prompted = False
    funit = -1
    parm = ' '
    ddid = ' '
    loc_tz_cd = in_loc_tz_cd
    if loc_tz_cd == ' ': loc_tz_cd = 'LOC'

    if intrans[0] == ' ':
        transport_cd = ' '
        sensor_type_id = NW_NI4
    else:
        transport_cd = intrans[o].upper()
        sensor_type_id = 0

    if not nw_write_log_entry(1):
        nw_write_error(6)
        irc = nw_get_error_number()
        999()                   # ex-GOTO

    if indbnum == ' ':
        rtdbnum = 1
    else:
        # TODO: error-handle non-digit indbnum values here
        rtdbnum = indbnum

    # Load db number into some common blocks
    dbnum = rtdbnum
    dbnumb = rtdbnum
    s_dbget(1, rtdbnum, irc)

    # Set control file path
    if len(ctlpath) > 128: 998() # ex-GOTO
    ctlfile = ctlpath

    # set logical flags
    rndsup   = ((inrndsup == 'y') or (inrndsup == 'Y'))
    wyflag   = ((inwyflag == 'y') or (inwyflag == 'Y'))
    cflag    = ((incflag == 'y')  or (incflag == 'Y'))
    vflag    = ((invflag == 'y')  or (invflag == 'Y'))
    multiple = ((inmultiple == 'y') or (inmultiple == 'Y'))
    hydra    = ((inhydra == 'y')  or (inhydra == 'Y'))
    addkey = (ctlfile != ' ' and not multiple)

    # check for a control file

    if ctlfile != ' ':          # using a control file - open it
        irc = nwc_rdb_cfil(one, ctlfile, rtagny, sid, ddid, 
                           stat, bctdtm, ectdtm, nline)
    #5
    if irc != 0: 999()          # ex-GOTO
    s_date(cdate, ctime)
    # WRITE (0,2010) cdate, ctime, ctlfile(1:nwf_strlen(ctlfile))
    #2010     FORMAT (A8,1X,A6,1X,'Processing control file: ',A)
    print cdate + " " + ctime + " Processing control file: " + ctlfile
    #  get a line from the file
    first = True
    irc = rdb_cfil(two, datatyp, rtagny, sid, ddid, stat,
                   bctdtm, ectdtm, nline)

    #6
    if irc != 0:
        irc = nwc_rdb_cfil(three, ctlfile, rtagny, sid, ddid, 
                           stat, bctdtm, ectdtm, nline)
    if not first:               # end of control file
        s_mclos                 # close things down and exit cleanly
    if funit >= 0 and funit != 6:
        s_sclose(funit, 'keep')
    irc = 0

    goto_999()

    #  check data type
    if hydra:
        if datatyp != 'DV' and datatyp != 'UV' and \
           datatyp != 'MS' and datatyp != 'WL' and \
           datatyp != 'QW':
            s_date (cdate, ctime)
            # WRITE (0,2020) cdate, ctime, datatyp, nline
            #2020           FORMAT (A8, 1X, A6, 1X, 'Invalid HYDRA data type "', A,
            #                 '" on line ', I5, '.')
            print cdate + " " + ctime + " Invalid HYDRA data type \"" + \
                datatyp + "\" on line " + nline
            goto_9()
    else:
        if datatyp != 'DV' and datatyp != 'UV' and \
           datatyp != 'DC' and datatyp != 'SV' and \
           datatyp != 'MS' and datatyp != 'PK':
            s_date (cdate, ctime)
            # WRITE (0,2021) cdate, ctime, datatyp, nline
            #2021           FORMAT (A8, 1X, A6, 1X, 'Invalid data type "', A,
            #                 '" on line ', I5, '.')
            print cdate + " " + ctime + " Invalid data type \"" + \
                datatyp + "\" on line " + nline
            goto_9()

    # check for completeness
    if rtagny == ' ' or sid == ' ' or \
       (datatyp != 'DC'. AND. datatyp != 'SV' and \
        datatyp != 'WL' and datatyp != 'QW' and \
        stat == ' ') or \
       begdtm == ' ' or enddtm == ' ' or \
       (datatyp != 'MS' and datatyp != 'PK' and \
        datatyp != 'WL' and datatyp != 'QW' and \
        ddid == ' '):
        s_date(cdate, ctime)
        # WRITE (0,2030) cdate, ctime, nline
        #2030        FORMAT (A8, 1X, A6, 1X, 'Incomplete row (missing items)',
        #              ' on line ', I5, '.')
        print cdate + " " + ctime + \
            " Incomplete row (missing items) on line " + nline
        goto_9()

    # zero pad stat code IF type is DV
    if datatyp == 'DV':
        s_jstrrt(stat, 5)
        stat.replace(' ', '0')

    if first:
        savetyp = datatyp     # save first data type
        mssav = stat[0]
        first = False
        s_lgid()              # get user id and number
        s_ndget()             # get node data
        s_ggrp()              # get groups (for security)
        sen_dbop(rtdbnum)     # open Midas files
        if not multiple:      # open output file
            if outpath == ' ':
                funit = 6
            else:
                if len(outpath) > 128:
                    irc = nwc_rdb_cfil(three, ctlfile, rtagny, sid,
                                       ddid, stat, bctdtm, ectdtm, nline) 
                    goto_998()

                rdbfile = outpath
                # TODO:
                # s_file(' ', rdbfile, ' ', 'unknown', 'write',
                #       0, 1, ipu, funit, irc, *7)
                #7
                if irc != 0:
                    irc = nwc_rdb_cfil (three, ctlfile, rtagny, sid, 
                                        ddid, stat, bctdtm, ectdtm, nline)
                    goto_999

    # IF multiple not specified, all requests must
    # be the same data type as the first one

    if not multiple:

      if datatyp != savetyp:
          s_date(cdate, ctime)
          # WRITE (0,2040) cdate, ctime, datatyp, savetyp, nline
          # 2040           FORMAT (A8, 1X, A6, 1X, 'Datatype of "', A,
          #            '" not the same as the first request datatype of "',
          #            A,'" on line ',I5,'.')
          print cdate + " " + ctime + " Datatype of \"" + datatyp + \
              "\" not the same as the first request datatype of \"" + \
              savetyp + "\" on line " + nline + "."
          goto_9

      if datatyp == 'MS' and stat[0] != mssav:
          #  can't mix types of CSG measurements
          s_date(cdate, ctime)
          # WRITE (0,2050) cdate,ctime,stat(1:1),mssav,nline
          #2050           FORMAT (A8,1X,A6,1X,'Measurement type of "',A,
          #              '" not compatible with the first ',
          #              'measurement type of "',
          #              A,'" on line ',I5,'.')
          print cdate + " " + ctime + " " + \
              "Measurement type of \"" + stat[0] + \
              "\" not compatible with the first measurement type of \"" + \
              mssav + "\" on line " + nline + "."
          goto_9

    # convert water years to date or datetime if -w specified
    nw_rdb_fill_beg_dtm(wyflag, bctdtm, begdtm)
    nw_rdb_fill_end_dtm(wyflag, ectdtm, enddtm)

    # if multiple, open a new output file - outpath is a prefix

    if multiple:                # close previously open file
        if funit >= 0 and funit != 6:
            s_sclose(funit, 'keep')
        #  open a new file
        rdbfile = outpath + "." + datatyp + "." + rtagny + "." + sid
        rdblen = len(outpath) + 5 + len(rtagny) + len(sid)
        if datatyp != 'MS' and datatyp != 'PK' and \
           datatyp != 'WL' and datatyp != 'QW':
            lddid = ddid
            s_jstrlf(lddid, 4)
            rdbfile = rdbfile + "." + lddid
            rdblen = rdblen + 1 + len(lddid)

        if datatyp != 'DC' and datatyp != 'SV' and \
           datatyp != 'WL' and datatyp != 'QW':
            rdbfile = rdbfile + '.' + stat
            rdblen = rdblen + 1 + stat

        rdbfile = rdbfile + '.' + begdtm[0:7] + '.rdb'
        rdblen = rdblen + 13
        # TODO:
        #s_file (' ', rdbfile, ' ', 'unknown', 'write',
        #        0, 1, ipu, funit, irc, *8)
        #8
        if irc != 0:
            s_date (cdate, ctime)
            #       WRITE (0,2060) cdate, ctime, irc, nline,
            #                                rdbfile(1:rdblen)
            #2060           FORMAT (A8, 1X, A6, 1X, 'Error ', I5,
            #             ' opening output file for line ',
            #             I5, '.', /, 16X, A)
            print cdate + " " + ctime + " Error " + irc + \
                " opening output file for line " + nline + ".\n" + \
                rdbfile[0:rdblen - 1]

            irc = nwc_rdb_cfil(three, ctlfile, rtagny, sid, ddid, 
                               stat, bctdtm, ectdtm, nline)
            s_mclos
            goto_999

        s_date(cdate, ctime)
        #       WRITE (0,2070) cdate, ctime, rdbfile(1:rdblen)
        #2070        FORMAT (A8, 1X, A6, 1X, 'Writing file ', A)
        print cdate + " " + ctime + " Writing file " + rdbfile

    # check DD for a P in column 1 - indicated parm code for PR DD search

    if ddid[0] == 'p' or ddid[0] == 'P':
        parm = ddid[1:5]
        s_jstrrt(parm, 5)
        parm = parm.replace(' ', '0')

        get_prdd(rtdbnum, rtagny, sid, parm, ddid, irc)
        if irc != 0:
            s_date(cdate, ctime)
            #       WRITE (0,2035) cdate, ctime, rtagny, sid, parm, nline
            #2035           FORMAT (A8, 1X, A6, 1X, 'No PRIMARY DD for station "', 
            #                         A5, A15, '", parm "', A5, '" on line ', I5, '.')
            print cdate + " " + ctime + \
                " No PRIMARY DD for station \"" + rtagny + sid + \
                "\", parm \"" + parm + "\" on line " + nline
            goto_9
        else:
            if datatyp != 'MS' and datatyp != 'PK' and \
               datatyp != 'WL' and datatyp != 'QW':
                # right justify DDID to 4 characters
                s_jstrrt(ddid, 4)

    # process the request
    if datatyp == "DV":
        fdvrdbout(funit, False, rndsup, addkey, vflag, 
                  cflag, rtagny, sid, ddid, stat, 
                  begdtm, enddtm, irc)
    elif datatyp == "UV":
        uvtyp = stat[0]
        if uvtyp != 'M' and uvtyp != 'N' and uvtyp != 'E' \
           and uvtyp != 'R' and uvtyp != 'S' and \
           uvtyp != 'C':
            s_date (cdate, ctime)
            # WRITE (0,2080) cdate, ctime, uvtyp, nline
            #2080           FORMAT (A8, 1X, A6, 1X, 'Invalid unit-values type "', 
            #         A1, '" on line ', I5,'.')
            print cdate + " " + ctime + \
                " Invalid unit-values type \"" + uvtyp + \
                "\" on line " + nline
        else:
           if uvtyp == 'M':
               inguvtyp = "meas"
           if uvtyp == 'N':
               inguvtyp = "msar"
           if uvtyp == 'E':
               inguvtyp = "edit"
           if uvtyp == 'R':
               inguvtyp = "corr"
           if uvtyp == 'S':
               inguvtyp = "shift"
           if uvtyp == 'C':
               inguvtyp = "da"
           fuvrdbout(funit, False, rtdbnum, rndsup, cflag,
                     vflag, addkey, rtagny, sid, ddid,  
                     inguvtyp, sensor_type_id, transport_cd,
                     begdtm, enddtm, loc_tz_cd, irc)
    elif datatyp == "MS":
        mstyp = stat[0]

        # Only standard meas types allowed when working from a
        # control file

        # Pseudo-UV Types 1 through 3 are only good from the
        # command line or in Hydra mode

        if mstyp != 'C' and mstyp != 'M' and mstyp != 'D' and mstyp != 'G': 
            s_date(cdate, ctime)
            #       WRITE (0,2090) cdate, ctime, mstyp, nline
            #2090           FORMAT (A8, 1X, A6, 1X,
            #          'Invalid measurement file type "', A1,
            #          '" on line ', I5, '.')
            print cdate + " " + ctime + \
                " Invalid measurement file type \"" + mstyp + \
                "\" on line " + nline + "."
        else:
            fmsrdbout(funit, rtdbnum, rndsup, addkey, cflag,
                      vflag, rtagny, sid, mstyp, begdtm, 
                      enddtm, irc)
    elif datatyp == "PK":
       pktyp = stat[0]
       if pktyp != 'F' and pktyp != 'P' and pktyp != 'B':
           s_date (cdate, ctime)
           # WRITE (0,2100) cdate, ctime, pktyp, nline
           #2100           FORMAT (A8,1X,A6,1X,'Invalid peak flow file type "',A1,
           #           '" on line ',I5,'.')
           print cdate + " " + ctime + \
               " Invalid peak flow file type \"" + pktyp + \
               "\" on line " + nline + "."
       else:
           fpkrdbout(funit, rndsup, addkey, cflag, vflag,
                     rtagny, sid, pktyp, begdtm, enddtm, irc)
    elif datatyp == "DC":
        fdcrdbout(funit, rndsup, addkey, cflag, vflag,
                  rtagny, sid, ddid, begdtm, enddtm,
                  loc_tz_cd, irc)
    elif datatyp == "SV":
        fsvrdbout(funit, rndsup, addkey, cflag, vflag, \
                  rtagny, sid, ddid, begdtm, enddtm, \
                  loc_tz_cd, irc)

        #  get next line from control file
        #9
        irc = rdb_cfil(two, datatyp, rtagny, sid, ddid, stat, \
                       bctdtm, ectdtm, nline)
        goto_6
    else:
        # Not a control file

        sopt = "10000000000000000000000000000000" # init control argument
        if len(intyp) > 2:
            datatyp = intyp[0:1]
        else:
            datatyp = intyp

        # check data type
        s_upcase(datatyp, 2)

        if hydra:
            needstrt = True
            sopt[7] = '1'
            sopt[11] = '2'
            if datatyp != 'DV' and datatyp != 'UV' and \
               datatyp != 'MS' and datatyp != 'WL' and \
               datatyp != 'QW':
                datatyp = 'UV'

                # convert dates to 8 characters
                rdb_fill_beg_date(wyflag, begdat, begdate)
                rdb_fill_end_date(wyflag, enddat, enddate)

                # convert date/times to 14 characters
                rdb_fill_beg_dtm(wyflag, begdat, begdtm)
                rdb_fill_end_dtm(wyflag, enddat, enddtm)

        else:

            if cflag:
                # Data type VT is pseudo-UV, no combining of date time
                # possible

                if datatyp != 'DV' and datatyp != 'UV' and \
                   datatyp != 'DC' and datatyp != 'SV' and \
                   datatyp != 'MS' and datatyp != 'PK' and \
                   datatyp != 'WL' and datatyp != 'QW':
       
                    #11
                    datatyp = ' '
                    print "Valid data types are:"
                    print "   DV - Daily Values"
                    print "   UV - Unit Values"
                    print "   MS - Discharge Measurements"
                    print "   PK - Peak Flows"
                    print "   DC - Data Corrections"
                    print "   SV - Variable Shift"
                    print "   WL - Water Levels from GWSI"
                    print "   QW - QW Data From QWDATA"
                    # TODO: 
                    # s_qryc('Enter desired data type: ',' ',0,0,2,2,
                    #                      datatyp,*11)
                    datatyp = datatyp.upper()
                    if datatyp != 'DV' and datatyp != 'UV' and \
                       datatyp != 'DC' and datatyp != 'SV' and \
                       datatyp != 'MS' and datatyp != 'PK' and \
                       datatyp != 'WL' and datatyp != 'QW':
                        # TODO:
                        s_bada(
                            "Please answer " +
                            "\"DV\", \"UV\", \"MS\", \"PK\", \"DC\", \"ST\"," + 
                            " \"SV\", \"WL\" or \"QW\".", *11
                        ) 

            else:

                if datatyp != 'DV' and datatyp != 'UV' and \
                   datatyp != 'DC' and datatyp != 'SV' and \
                   datatyp != 'MS' and datatyp != 'VT' and \
                   datatyp != 'PK' and datatyp != 'WL' and \
                   datatyp != 'QW':

                    #12
                    datatyp = ' '
                    print "Valid data types are:"
                    print "   DV - Daily Values"
                    print "   UV - Unit Values"
                    print "   MS - Discharge Measurements"
                    print "   VT - Site Visit Readings"
                    print "   PK - Peak Flows"
                    print "   DC - Data Corrections"
                    print "   SV - Variable Shift"
                    print "   WL - Water Levels from GWSI"
                    print "   QW - QW Data From QWDATA"
                    # TODO:
                    # s_qryc('Enter desired data type: ',' ',0,0,2,2,
                    #     datatyp,*12)
                    datatyp = datatyp.upper()
                    if datatyp != "DV" and datatyp != "UV" and \
                       datatyp != "DC" and datatyp != "SV" and \
                       datatyp != "MS" and datatyp != "VT" and \
                       datatyp != "PK" and datatyp != "WL" and \
                       datatyp != "QW":
                        # TODO:
                        s_bada(
                            "Please answer " +
                            "\"DV\", \"UV\", \"MS\", \"VT\", \"PK\", \"DC\", \"ST\"," + 
                            " \"SV\", \"WL\" or \"QW\".",
                            *12
                        )

        # convert agency to 5 characters - default to USGS
        if inagny == ' ':
            rtagny = 'USGS'
        else:
            if len(inagny) > 5:
                rtagny = inagny[0:4]
            else:
                rtagny = inagny
            s_jstrlf(rtagny, 5)

        # convert station to 15 characters
        if instnid == ' ':
            needstrt = True
            if datatyp == 'MS' or datatyp == 'PK' or \
               datatyp == 'WL' or datatyp == 'QW':
                sopt[4] = '1'
            else:
                sopt[4] = '2'
        else:
            if len(instnid) > 15:
                sid = instnid[0:14]
            else:
                sid = instnid
            s_jstrlf(sid, 15)

        # DD is ignored for data types MS, PR, WL, and QW

        if datatyp != 'MS' and datatyp != 'PK' and \
           datatyp != 'WL' and datatyp != 'QW':

            # If type is VT, DDID is only needed IF parm and loc
            # number are not specified
            if (datatyp != 'VT' and inddid == ' ') \
               or \
               (datatyp == 'VT' and inddid == ' ' \
                and (inddid[0] != 'P' or inlocnu == ' ')):
                needstrt = True
                sopt[4] = '2'

        else:

            # If ddid starts with "P", it is a parameter code, fill to
            # 5 digits
            if inddid[0] == 'p' or inddid[0] == 'P':
                if len(inddid) > 6:
                    parm = inddid[1:5]
                else:
                    parm = inddid[1:]
                s_jstrrt(parm, 5)
                parm.replace(' ', '0')
            else:
                parm = ' '
                # convert ddid to 4 characters
                if len(inddid) > 4:
                    ddid = inddid[0:3]
                else:
                    ddid = inddid
                s_jstrrt(ddid, 4)

    # further processing depends on data type

    if datatyp == 'DV':
        # convert stat to 5 characters
        if instat == ' ':
            needstrt = True
            sopt[7] = '1'
        else:
            if len(instat) > 5:
                stat = instat[0:4]
            else:
                stat = instat
            s_jstrrt (stat,5)
            stat.replace(' ', '0')

    if datatyp == 'DV' or datatyp == 'DC' or \
       datatyp == 'SV' or datatyp == 'PK':

        # convert dates to 8 characters
        if begdat == ' ' or enddat == ' ':
            needstrt = True
            if wyflag:
                sopt[8] = '4'
            else:
                sopt[9] = '3'
        else:
            rdb_fill_beg_date (wyflag, begdat, begdate)
            rdb_fill_end_date (wyflag, enddat, enddate)

    if datatyp == 'UV':

        if not hydra:
            # get UV type
            uvtyp = instat[0]
            if uvtyp == 'm': uvtyp = 'M'
            if uvtyp == 'n': uvtyp = 'N'
            if uvtyp == 'e': uvtyp = 'E'
            if uvtyp == 'r': uvtyp = 'R'
            if uvtyp == 's': uvtyp = 'S'
            if uvtyp == 'c': uvtyp = 'C'
            if uvtyp != 'M' and uvtyp != 'N' and \
               uvtyp != 'E' and uvtyp != 'R' and \
               uvtyp != 'S' and uvtyp != 'C':
                uvtyp_prompted = True
                #50
                uvtyp = ' '
                s_qryc(
                    "Unit values type (M, N, E, R, S, or C): ",
                    ' ', 0, 0, 1, 1, uvtyp, *50
                )
                if uvtyp == 'm': uvtyp = 'M'
                if uvtyp == 'n': uvtyp = 'N'
                if uvtyp == 'e': uvtyp = 'E'
                if uvtyp == 'r': uvtyp = 'R'
                if uvtyp == 's': uvtyp = 'S'
                if uvtyp == 'c': uvtyp = 'C'
                if uvtyp != 'M' and uvtyp != 'N' and \
                   uvtyp != 'E' and uvtyp != 'R' and \
                   uvtyp != 'S' and uvtyp != 'C':
                    s_bada (
                         "Please answer \"M\", \"N\", \"E\", \"R\", \"S\", or \"C\".",
                         *50
                    )

            # convert date/times to 14 characters
            if begdat == ' ' or enddat == ' ':
                needstrt = True
                if wyflag:
                    sopt[8] = '4'
                else:
                    sopt[9] = '3'
            else:
                rdb_fill_beg_dtm(wyflag, begdat, begdtm)
                rdb_fill_end_dtm(wyflag, enddat, enddtm)


        # If Hydra mode for UV data, set time zone code that in effect
        # for the first date for this station

        if hydra and datatyp != 'UV':
            if not nw_key_get_zone_dst(rtdbnum, rtagny, sid, tz_cd, local_time_fg):
                loc_tz_cd = 'UTC' # default to UTC
            else:
                if not nw_get_dflt_tzcd(tz_cd, local_time_fg, begdtm[0:7], loc_tz_cd):
                    loc_tz_cd = 'UTC' # default to UTC

        if datatyp == 'MS':     # get MS type
            mstyp = instat[0].upper()

        if mstyp != 'C' and mstyp != 'M' and \
           mstyp != 'D' and mstyp != 'G' and \
           mstyp != '1' and mstyp != '2' and \
           mstyp != '3':
            #45
            mstyp = ' '

            #1234
            print "Measurement file retrieval type -\n" + \
                "  C - Crest Stage Gage measurements,\n" + \
                "  M - Discharge Measurements,\n" + \
                "  D - Detailed Discharge Measurements,\n" + \
                "  G - Gage Inspections,\n" + \
                "  1 - Pseudo UV, measurement discharge,\n" + \
                "  2 - Pseudo UV, measurement stage, or\n" + \
                "  3 - Pseudo UV, mean index velocity"

            s_qryc("|Enter C, M, D, G, or 1 to 3: ",
                   ' ', 0, 0, 1, 1, mstyp, *45)
            mstyp.upper()
            if mstyp != 'C' and mstyp != 'M' and \
               mstyp != 'D' and mstyp != 'G' and \
               mstyp != '1' and mstyp != '2' and \
               mstyp != '3':
                s_bada(
                    "Please answer \"C\", \"M\", \"G\", or \"1\" to \"3\".",
                    *45
                )

        if begdat == ' ' or enddat == ' ':
            needstrt = True
            if wyflag:
                sopt[8] = '4'
            else:
                sopt[9] = '3'

        else:

            if mstyp >= '1' and mstyp <= '3':
                # doing pseudo-UV, convert date/times to 14 characters
                rdb_fill_beg_dtm (wyflag, begdat, begdtm)
                rdb_fill_end_dtm (wyflag, enddat, enddtm)
            else:
                # convert dates to 8 characters
                rdb_fill_beg_date (wyflag, begdat, begdate)
                rdb_fill_end_date (wyflag, enddat, enddate)

        if datatyp == 'VT':     # get VT type
            vttyp = instat[0]
            vttyp.upper()

            if vttyp != 'P' and vttyp != 'R' and \
               vttyp != 'A' and vttyp != 'M' and \
               vttyp != 'F':

                #55
                vttyp = 'A'

                #1235
                print "SiteVisit Pseudo UV readings " + \
                      "retrieval type -\n" + \
                      "  P - Retrieve sensor insp. " + \
                      "primary reference readings\n" + \
                      "  R - Retrieve sensor insp. " + \
                      "primary recorder readings\n" + \
                      "  A - Retrieve sensor insp. " + \
                      "all readings\n" + \
                      "  M - Retrieve QW monitor readings\n" + \
                      "  F - Retrieve QW field meter readings"

                s_qryc("|Enter P, R, A, M, or F (<CR> = A):",
                       ' ', 0, 1, 1, 1, vttyp, *55)
                vttyp.upper()
                if vttyp != 'P' and vttyp != 'R' and \
                   vttyp != 'A' and vttyp != 'M' and \
                   vttyp != 'F':
                    s_bada(
                        'Please answer "P", "R", "A", "M" or "F".',
                        *55
                    )

            # See if we have the date range
            if begdat == ' ' or enddat == ' ':
                needstrt = True
                if wyflag:
                    sopt[8] = '4'
                else:
                    sopt[9] = '3'

            # Doing pseudo-UV, convert date/times to 14 characters
            rdb_fill_beg_dtm(wyflag, begdat, begdtm)
            rdb_fill_end_dtm(wyflag, enddat, enddtm)

        if datatyp == 'PK':     # get pk type

            pktyp = instat[0]
            if pktyp == 'f': pktyp = 'F'
            if pktyp == 'p': pktyp = 'P'
            if pktyp == 'b': pktyp = 'B'

            if pktyp != 'F' and pktyp != 'P' and pktyp != 'B':
                #46
                pktyp = ' '
                s_qryc('Peak flow file retrieval type -' +
                       '|Full peaks only (F),' +
                       '|Partial peaks only (P),' +
                       '|Both Full and Partial peaks (B) - ' +
                       '|Please enter F, P, or B: ',' ',0,0,1,1,
                       pktyp, *46)
                if pktyp == 'f': pktyp = 'F'
                if pktyp == 'p': pktyp = 'P'
                if pktyp == 'b': pktyp = 'B'
                if pktyp != 'F' and pktyp != 'P' and pktyp != 'B':
                    s_bada (
                        'Please answer "F", "P",  or "B".', *46)

        if datatyp == 'WL':
            wltyp = instat[0]
            if not (wltyp >= '1' and wltyp <= '3'): wltyp = ' '
            # convert date/times to 14 characters
            if begdat == ' ' or enddat == ' ':
                needstrt = True
                sopt[4] = '1'
                if wyflag:
                    sopt[8] = '4'
                else:
                    sopt[9] = '3'
            else:
                rdb_fill_beg_dtm(wyflag, begdat, begdtm)
                rdb_fill_end_dtm(wyflag, enddat, enddtm)

        if datatyp == 'QW':
            qwparm = ' '
            if len(inddid) >= 2:
                qwparm = inddid[1:]
                qwmeth = instat
            # convert date/times to 14 characters
            if begdat == ' ' or enddat == ' ':
                needstrt = True
                sopt[4] = '1'
                if wyflag:
                    sopt[8] = '4'
                else:
                    sopt[9] = '3'
            else:
                rdb_fill_beg_dtm(wyflag, begdat, begdtm)
                rdb_fill_end_dtm(wyflag, enddat, enddtm)

        if needstrt:               # call s_strt if needed
            s_mdus(nw_oprw, irc, *998) # get USER info 
            if irc != 0:
                #2110
                print 'Unable to open ADAPS User file - Aborting.\n'
                goto_998()
        s_lgid()                # get user info 
        s_mdus(nw_read, irc, *998)
        if irc == 0:            # save the user info
            holdbuff = usbuff[0:90]
            dbnum = rtdbnum     # load supplied parts of user info
            if sopt[4] == '1' or sopt[4] == '2':
                agency = rtagny
                if instnid != ' ': stnid = sid
            s_mdus(nw_updt, irc, *998) # save modified user info

        # call start routine
        prgid = 'aq2rdb'
        if titlline  == ' ':
            prgdes = 'Time-series to RDB Output'
        else:
            if len(titlline) > 80:
                prgdes = titlline[0:79]
            else:
                prgdes = titlline
        rdonly = 1
        #123
        s_strt(sopt, *998)
        sopt[0] = '2'
        rtdbnum = dbnum         # get DB number first

        if sopt[4] == '1' or sopt[4] == '2':
            rtagny = agency     # get agency
            sid = stnid         # get stn ID
            if sopt[4] == '2':
                ddid = usddid   # and DD number

        if ddid == ' ':
            if parm != ' ' and datatyp != 'VT':
                nwf_get_prdd(rtdbnum, rtagny, sid, parm, ddid, irc)
                if irc != 0:
                    write_2120(rtagny, sid, parm)
                    goto_999()

        # stat code
        if sopt[7] == '1': stat = statcd

        # data type
        if sopt[11] == '2':
            uvtyp_prompted = True
        if usdtyp == 'D':
            datatyp = 'DV'
            cflag = False
        elif usdtyp == 'V':
            datatyp = 'DV'
            cflag = True
        elif usdtyp == 'U':
            datatyp = 'UV'
            uvtyp = 'M'
        elif usdtyp == 'N':
            datatyp = 'UV'
            uvtyp = 'N'
        elif usdtyp == 'E':
            datatyp = 'UV'
            uvtyp = 'E'
        elif usdtyp == 'R':
            datatyp = 'UV'
            uvtyp = 'R'
        elif usdtyp == 'S':
            datatyp = 'UV'
            uvtyp = 'S'
        elif usdtyp == 'C':
            datatyp = 'UV'
            uvtyp = 'C'
        elif usdtyp == 'M':
            datatyp = 'MS'
        elif usdtyp == 'X':
            datatyp = 'VT'
        elif usdtyp == 'L':
            datatyp = 'WL'
        elif usdtyp == 'Q':
            datatyp = 'QW'

        # date range for water years
        if sopt[8] == '4':
            if usyear == '9999':
                begdtm = '00000000000000'
                begdate = '00000000'
            else:
                # TODO:
                #READ (usyear,1010) iyr
                #1010              FORMAT (I4)
                # WRITE (usdate,2140) iyr-1,10,01
                #2140              FORMAT (I4.4,2I2.2)
                begdtm = usdate + '000000'
                begdate = usdate
            if ueyear == '9999':
                enddtm = '99999999999999'
                enddate = '99999999'
            else:
                # TODO:
                #READ (ueyear,1010) iyr
                #WRITE (uedate,2140) iyr,9,30
                enddtm = uedate + '235959'
                enddate = uedate

        # date range
        if sopt[9] == '3':
            begdate = usdate
            enddate = uedate
            begdtm = usdate + '000000'
            if uedate == '99999999':
                enddtm = '99999999999999'
            else:
                enddtm = uedate + '235959'

        # Restore contents of user buffer
        if irc == 0:
            usbuff = holdbuff[0:90]
            s_mdus(nw_updt, irc, *998)

    else:

        s_lgid()                 # get user id and number
        s_ndget()                # get node data
        s_ggrp()                 # get groups (for security)
        sen_dbop(rtdbnum)        # open Midas files
        # count program (counted by S_STRT above if needed)
        # TODO: translate F77 in condition below
        #if not nw_db_save_program_info('aq2rdb'):
            # continue      # ignore errors, we don't care if not counted
        if parm != ' ' and datatyp != 'VT':
            # get PRIMARY DD that goes with parm if parm supplied
            nwf_get_prdd(rtdbnum, rtagny, sid, parm, ddid, irc)
        if irc != 0:
            write_2120(rtagny, sid, parm)
            goto_999

        # retrieving measured UVs and transport_cd not supplied,
        # prompt for it
        if uvtyp_prompted and datatyp == 'UV' and \
           (uvtyp == 'M' or uvtyp == 'N') and \
           transport_cd == ' ':
            nw_query_meas_uv_type(rtagny, sid, ddid, begdtm,
                                  enddtm, loc_tz_cd, transport_cd,
                                  sensor_type_id, *998)
            if transport_cd == ' ':
                print "No MEASURED UV data for station \"" + rtagny + \
                    sid + "\", DD \"" + ddid + "\". Aborting."
                goto_999()

        # Open output file
        if outpath == ' ':
            funit = 6
        else:
            if len(outpath) > 128: goto_998()
            rdbfile = outpath
            s_file(' ', rdbfile, ' ', 'unknown', 'write', 0, 1,
                   ipu, funit, irc, *90)
            #90
            if irc != 0:
                print "Error opening output file:\n" + \
                    "   " + rdbfile
                goto_999()

        # get data and output to files

        if datatyp == 'DV':

            fdvrdbout(funit, False, rndsup, addkey, vflag,
                      cflag, rtagny, sid, ddid, stat, 
                      begdate, enddate, irc)

        elif datatyp == 'UV':

            if uvtyp == 'M': inguvtyp = 'meas'
            if uvtyp == 'N': inguvtyp = 'msar'
            if uvtyp == 'E': inguvtyp = 'edit'
            if uvtyp == 'R': inguvtyp = 'corr'
            if uvtyp == 'S': inguvtyp = 'shift'
            if uvtyp == 'C': inguvtyp = 'da'

            fuvrdbout(funit, False, rtdbnum, rndsup, cflag,
                      vflag, addkey, rtagny, sid, ddid, inguvtyp, 
                      sensor_type_id, transport_cd, begdtm, 
                      enddtm, loc_tz_cd, irc)

        elif datatyp == 'MS':

            if hydra or (mstyp >= '1' and mstyp <= '3'):
                if hydra: mstyp = ' '
                fmsrdbout_hydra(funit, rndsup, rtagny, sid,
                                begdtm, enddtm, loc_tz_cd, 
                                mstyp, irc)
            else:
                fmsrdbout(funit, rtdbnum, rndsup, addkey, cflag,
                          vflag, rtagny, sid, mstyp, begdate,  
                          enddate, irc)

        elif datatyp == 'VT':

            # Get parm and location number from DD, if not specified
            # in arguments
            if inddid[0] != 'P' or inlocnu == ' ':
                if not nw_db_key_get_dd_parm_loc(rtdbnum, rtagny, 
                                                 sid, ddid, parm,
                                                 loc_nu):
                    goto_997
            else:
                loc_nu = nwc_atoi(inlocnu)

            fvtrdbout_hydra(funit, rndsup, rtagny, sid, parm,
                            loc_nu, begdtm, enddtm, loc_tz_cd,
                            vttyp, irc)

        elif datatyp == 'PK':
            fpkrdbout(funit, rndsup, addkey, cflag, vflag, 
                      rtagny, sid, pktyp, begdate, enddate, irc) 

        elif datatyp == 'DC':

            fdcrdbout(funit, rndsup, addkey, cflag, vflag,
                      rtagny, sid, ddid, begdate, enddate, 
                      loc_tz_cd, irc)

        elif datatyp == 'SV':

            fsvrdbout(funit, rndsup, addkey, cflag, vflag,
                      rtagny, sid, ddid, begdate, enddate, 
                      loc_tz_cd, irc)

        elif datatyp == 'WL':

            if hydra: wltyp = ' '
            fwlrdbout_hydra (funit, rndsup, rtagny, sid, begdtm,
                             enddtm, loc_tz_cd, wltyp, irc)

        elif datatyp == 'QW':

            if hydra:
                qwparm = ' '
                qwmeth = ' '

            fqwrdbout_hydra(funit, rndsup, rtagny, sid, begdtm,
                            enddtm, loc_tz_cd, qwparm, qwmeth,
                            irc)

    # close files and exit
    #997
    s_mclos()
    s_sclose(funit, 'keep')
    nw_disconnect()
    goto_999()

    # bad return (do a generic error message)
    #998
    irc = 3
    nw_error_handler(irc, 'nwf_rdb_out', 'error',
                     'doing something', 'something bad happened')

    goto_999()