# File - fdvrdbout.py
#
# Purpose - Python emulation of legacy NWIS, FDVRDBOUT() Fortran
#           subroutine: "Write DV data in rdb FORMAT" [sic].
#
# Authors - Andrew Halper <ashalper@usgs.gov> [Python translation]
#           Scott D. Bartholoma <sbarthol@usgs.gov> [FDVRDBOUT()]
#

# Python modules
import urllib, os, sys

# aq2rdb modules
import rdb_write_loc_info
rdb_write_loc_info = rdb_write_loc_info.rdb_write_loc_info

# TODO: this is a stub; see watstore/adaps/adrsrc/dd_lib/s_lbdd.f
nw_left = 0
def s_lbdd(justify):
    text = ''
    return text

# TODO: this is a stub; see watstore/library/wat_lib/pmretr.sf
def pmretr(ifunct):
    ierr = 0
    return ierr

# TODO: this is a stub; see
# watstore/adaps/adrsrc/tsing_lib/nw_db_retr_dvabort.sf
def db_retr_dvabort( agency_cd, site_no, dd_nu, begdtm, enddtm):
    dvabort = 0
    return dvabort

# TODO: stub
def s_statck(stat):
    return 0

# TODO: stub; see watstore/adaps/adrsrc/rdb_lib/nw_rdb_header.f
def rdb_header(funit):
    return

# TODO: stub; see watstore/adaps/adrsrc/rdb_lib/nw_rdb_dbline.f
def rdb_dbline(funit):
    return

def fdvrdbout(
        funit,                  # file unit writing output into
        editable,               # flag IF DVs are editable
        rndsup,                 # rounding-suppressed flag
        addkey,                 # flag IF logical key to be added to each row
        vflag,                  # flag for Excel-style verbose dates/times
        compdv,                 # flag IF retrieving computed DVs only
        agyin,                  # agency code
        station,                # station number
        inddid,                 # input DD number
        stat,                   # statistics code
        begdate,                # start date
        enddate                 # end date
):
    #  *********************************************************************
    #   ARGUMENT DECLARATIONS
    #  *********************************************************************

    # INTEGER funit

    # LOGICAL editable,
    #*        rndsup,
    #*        addkey,
    #*        vflag,
    #*        compdv

    # CHARACTER agyin*5,
    #*          station*15,
    #*          inddid*4,
    #*          stat*5,
    #*          begdate*8,
    #*          enddate*8

    # INTEGER rtcode

    #  *********************************************************************
    #   FUNCTION DECLARATIONS
    #  *********************************************************************

    # LOGICAL s_lpyr,
    #*        nw_cdt_ok,
    #*        nw_getchk_table_nm,
    #*        nw_db_retr_dvabort,
    #*        nw_va_rget_rd,
    #*        nw_va_rrnd_tx,
    #*        nw_db_retr_dv_first_yr,
    #*        nw_db_retr_dv_last_yr,
    #*        nw_db_retr_aging_for_date,
    #*        nw_no_data_found

    # INTEGER nwf_strlen,
    #*        nw_get_error_number

    #  *********************************************************************
    #   EXTERNAL SUBROUTINES OR FUNCTIONS
    #  *********************************************************************

    # EXTERNAL s_lpyr,
    #*         nw_cdt_ok,
    #*         nw_getchk_table_nm,
    #*         nwf_strlen,
    #*         nw_db_retr_dvabort,
    #*         nw_va_rget_rd,
    #*         nw_va_rrnd_tx

    #  *********************************************************************
    #   INCLUDE FILES
    #  *********************************************************************

    # INCLUDE 'user_data.ins'
    # INCLUDE 'adaps_keys.ins'
    # INCLUDE 'ins.district_data'
    # INCLUDE 'sitecomm.ins'
    smgtof = '      '           # Time zone code (Offset from UTC)
    slstfl = ' '                # Daylight savings time flag
    # INCLUDE 'parmcomm.ins'
    # INCLUDE 'statcomm.ins'
    # INCLUDE 'ins.dddata'
    # INCLUDE 'ins.processor_data'
    # INCLUDE 'ins.dv_data'

    #  *********************************************************************
    #   SQL VARIABLE DECLARATIONS
    #  *********************************************************************

    # EXEC SQL INCLUDE SQLCA

    # EXEC SQL DECLARE pstmt STATEMENT
    # EXEC SQL DECLARE cur_stmt CURSOR FOR pstmt
    # EXEC SQL BEGIN DECLARE SECTION

    #    CHARACTER  stmt*1024,
    #*              dv_dt*25,
    #*              dv_rd*1,
    #*              dv_rmk_cd*1,
    #*              dv_type_cd*1,
    #*              data_aging_cd*1

    #    EXEC SQL VAR dv_rd IS CHARF
    #    EXEC SQL VAR dv_rmk_cd IS CHARF
    #    EXEC SQL VAR dv_type_cd IS CHARF
    #    EXEC SQL VAR data_aging_cd IS CHARF

    #    REAL dv_va

    #    INTEGER*2  dv_va_null,
    #*              dv_rd_null

    # EXEC SQL END DECLARE SECTION

    #  *********************************************************************
    #   LOCAL VARIABLE DECLARATIONS
    #  *********************************************************************

    # INTEGER irc,
    #*     outlen,
    #*     dvabort,
    #*     ldv_name,
    #*     ldv_data_name,
    #*     ldv_diff_name,
    #*     rowcount,
    #*     dv_water_yr,
    #*     dtlen,
    #*     tunit

    # CHARACTER dv_name*64,
    #*     dv_data_name*64,
    #*     dv_diff_name*64,
    #*     cdd_id*12,
    #*     ddlabl*80,
    #*     cval*20,
    #*     cdate*8,
    #*     odate*8,
    #*     outlin*128,
    #*     rndary*10,
    #*     rndparm*10,
    rnddd = '          '
    #*     cdvabort*6,
    #*     bnwisdt*8,
    #*     enwisdt*8,
    #*     bnwisdtm*25,
    #*     enwisdtm*25,
    #*     bingdt*25,
    #*     eingdt*25,
    #*     temppath*256,
    #*     nullval*20,
    #*     nullrd*1,
    #*     nullrmk*1,
    #*     nulltype*1,
    #*     nullaging*1

    # CHARACTER exdate*10,        ! verbose Excel style date "mm/dd/yyyy"
    #&          dtcolw*3          ! holds DATE column width '19D' etc

    # LOGICAL wrotedata,
    #*     first

    #  *********************************************************************
    #   EQUIVALENCES
    #  *********************************************************************

    # EQUIVALENCE (pround(1),rndparm),
    #&            (altrnd(1),rnddd)

    #  *********************************************************************
    #   SAVES
    #  *********************************************************************

    # SAVE ddlabl, rndary, cdvabort, first

    #  *********************************************************************
    #   INITIALIZATIONS
    #  *********************************************************************

    # DATA ddlabl    / ' ' /
    # DATA rndary    / ' ' /
    # DATA cdvabort  / ' ' /
    # DATA first     / .TRUE. /
    # DATA nullval   / '**NULL**' /
    # DATA nullrd    / ' ' /
    # DATA nullrmk   / ' ' /
    # DATA nulltype  / ' ' /
    # DATA nullaging / ' ' /

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    #     initialize

    wrotedata = False

    if begdate == '00000000':
        # TODO: look up RH constant value in 3GL
        bnwisdtm = NW_NWIS_MINDATE
        bnwisdt = bnwisdtm[0:7]
    else:
        bnwisdtm = begdate + '000000'
        bnwisdt = begdate

    if enddate == '99999999':
        enwisdtm = NW_NWIS_MAXDATE
        enwisdt = enwisdtm[0:7]
    else:
        enwisdt = enddate
        enwisdtm = enddate + '23959'

    # get site
    sagncy = agyin
    sid = station

    # Query USGS Water Services for site attributes; see also
    # watstore/library/wat_lib/stretr.sf, and requestSite() in
    # ../aq2rdb.js
    url = 'http://waterservices.usgs.gov/nwis/site/?' + \
          urllib.urlencode({'format': 'rdb',
                            'sites': sid,
                            'siteOutput': 'expanded'})
    try:
        response = urllib.urlopen(url)
    except IOError as e:
        sys.stderr.write(
            os.path.basename(sys.argv[0]) +
            ': IOError from urllib.urlopen(\'' + url + '\'); ' +
            'the error message was: ' + os.strerror(e.errno) + '\n'
        )
        irc = e.errno
    else:
        if response.getcode() == 404:
            sagncy = agyin
            sid = station
            sname = '*** NOT IN SITE FILE ***'
            smgtof = ' '
            slstfl = ' '
            irc = response.getcode()
        else:
            data = response.read()
            irc = response.getcode()

    # parse RDB response
    table = data.split('\n')
    table.pop()                 # empty
    dataList = table.pop().split('\t')

    # TODO: these are indexed by field position now...bad; investigate
    # loading the site columns/values into a Python dictionary
    sname = dataList[2]         # SITEFILE.site_no
    smgtof = dataList[31]       # SITEFILE.tz_cd

    if smgtof == ' ':
        # from watstore/adaps/adrsrc/inserts/ins.dv_app:
        # 
        # INTEGER*4 GMTOF,          ! LOCAL OFFSET FROM GMT TIME IN HOURS
        smgtof = str(gmtof)     # WRITE(smgtof,'(I3)') gmtof

    smgtof = "{:<3}".format(smgtof) # s_jstrlf(smgtof, 3)
    if slstfl == ' ':
        # TODO: need to find out where LSTFG is declared in 3GL
        lstfg = 0
        if lstfg == 0:
            slstfl = 'Y'
        else:
            slstfl = 'N'

    # get DD
    ddagny = agyin
    ddstid = station
    ddid = inddid
    # TODO:
    #s_mddd(nw_read, irc, *998)

    # if HTTP success
    if 200 <= irc and irc < 300:
        ddlabl = s_lbdd(nw_left) # set label
        # TODO: find out where PMCODE is defined
        pcode = 'P' + pmcode    # set rounding

        # TODO: call parameter code Web service? See
        # https://internal.cida.usgs.gov/jira/browse/NWED-124
        rtcode = pmretr(60)
        if rnddd != ' ' and rnddd != '0000000000':
            rndary = rnddd
        else:
            rndary = rndparm
    else:
        ddagny = agyin
        ddstid = station
        ddid = inddid
        ddlabl = '*** NOT IN DD FILE ***'
        rndary = '9999999992'

    # DV abort limit defaults to 120 minutes
    cdvabort = '120'

    #  get the DV abort limit
    if db_retr_dvabort(ddagny, ddstid, ddid, bnwisdtm, enwisdtm):
        cdvabort = str(dvabort) # WRITE (cdvabort,'(I6)') dvabort
        cdvabort = "{:<6}".format(cdvabort) # s_jstrlf(cdvabort, 6)

    # get stat information
    irc = s_statck(stat)
    if irc != 0:
        ssnam = '*** INVALID STAT ***'

    if not addkey:

        # write the header records
        rdb_header(funit)

        if editable:
            # WRITE (funit, '(20A)')
            #             '# //FILE TYPE="NWIS-I DAILY-VALUES" ',
            #             'EDITABLE=YES'
            funit.write(
                '# //FILE TYPE="NWIS-I DAILY-VALUES" ' +
                'EDITABLE=YES\n'
            )
        else:
            # WRITE (funit, '(20A)')
            #             '# //FILE TYPE="NWIS-I DAILY-VALUES" ',
            #             'EDITABLE=NO'
            funit.write(
                '# //FILE TYPE="NWIS-I DAILY-VALUES" ' +
                'EDITABLE=NO\n'
            )

        # write database info
        rdb_dbline(funit)

        # write site info
        # WRITE (funit,'(20A)')
        #          '# //STATION AGENCY="',sagncy,'" NUMBER="',
        #          sid,'" TIME_ZONE="',
        #          smgtof(1:nwf_strlen(smgtof)),'" DST_FLAG=',
        #          slstfl
        funit.write(
            '# //STATION AGENCY="' + sagncy + '" NUMBER="' +
            sid + '" TIME_ZONE="' +
            smgtof + '" DST_FLAG=' +
            slstfl + '\n'
        )

        # WRITE (funit,'(20A)')
        #          '# //STATION NAME="',
        #          sname(1:nwf_strlen(sname)),'"'
        funit.write(
            '# //STATION NAME="' +
            sname + '"\n'
        )

        # write Location info
        # TODO: this is disabled right now, while we research how to
        # get Location info. from AQUARIUS
        #rdb_write_loc_info(funit, dd_id)

        # write DD info
        # WRITE (funit,'(20A)')
        #          '# //DD DDID="',ddid,'" RNDARY="',
        #          rndary,'" DVABORT=',
        #          cdvabort(1:nwf_strlen(cdvabort))
        funit.write(
            '# //DD DDID="' + ddid + '" RNDARY="' +
            rndary + '" DVABORT=' +
            cdvabort + '\n'
        )

        # WRITE (funit,'(20A)')
        #          '# //DD LABEL="',
        #          ddlabl(1:nwf_strlen(ddlabl)),'"'
        funit.write(
            '# //DD LABEL="' +
            ddlabl + '"\n'
        )

        # write parameter info
        # WRITE (funit,'(20A)')
        #        '# //PARAMETER CODE="',pcode(2:6),
        #        '" SNAME = "',psnam(1:nwf_strlen(psnam)),
        #        '"'
        # TODO: need to call PT's PARM Web service here to get
        # (parm_cd,parm_nm,parm_ds)
        funit.write(
            '# //PARAMETER CODE="' + pcode[1:5] +
            '" SNAME = "' + psnam +
            '"\n'
        )
        # WRITE (funit,'(20A)')
        #        '# //PARAMETER LNAME="',
        #        plname(1:nwf_strlen(plname)),'"'
        funit.write(
            '# //PARAMETER LNAME="' +
            plname + '"'
        )

        # write statistic info
        # WRITE (funit,'(20A)')
        #        '# //STATISTIC CODE="',scode(2:6),
        #        '" SNAME="',ssnam(1:nwf_strlen(ssnam)),
        #        '"'
        funit.write(
            '# //STATISTIC CODE="' + scode[1:5] +
            '" SNAME="' + ssnam +
            '"'
        )

        # WRITE (funit,'(20A)')
        #        '# //STATISTIC LNAME="',
        #        slname(1:nwf_strlen(slname)),'"'
        funit.write(
            '# //STATISTIC LNAME="' +
            slname + '"'
        )

        # write DV type info
        if compdv:
            # WRITE (funit,'(20A)')
            #           '# //TYPE NAME="COMPUTED" ',
            #           'DESC = "COMPUTED DAILY VALUES ONLY"'
            funit.write(
                '# //TYPE NAME="COMPUTED" ' +
                'DESC = "COMPUTED DAILY VALUES ONLY"'
            )
        else:
            # WRITE (funit,'(20A)')
            #           '# //TYPE NAME="FINAL" ',
            #           'DESC = "EDITED AND COMPUTED DAILY VALUES"'
            funit.write(
                '# //TYPE NAME="FINAL" ' +
                'DESC = "EDITED AND COMPUTED DAILY VALUES"'
            )

        # write data aging information
        nw_rdb_write_aging(funit, dbnum, dd_id, begdate, enddate)

        # write editable range
        # WRITE (funit,'(20A)')
        #        '# //RANGE START="', begdate,
        #        '" END="', enddate,'"'
        funit.write(
            '# //RANGE START="' + begdate +
            '" END="' + enddate + '"'
        )

        # write single site RDB column headings
        # WRITE (funit,'(20A)') outlin(1:46)
        funit.write('DATE\tTIME\tVALUE\tPRECISION\tREMARK\tFLAGS\tTYPE\tQA\n')

        if vflag:               # verbose Excel-style format
            dtcolw = '10D'      # "mm/dd/yyyy" 10 chars
            dtlen = 3
        else:
            dtcolw = '8D'       # "yyyymmdd" 8 chars
            dtlen=2

        # WRITE (funit,'(20A)') outlin(1:23+dtlen)
        funit.write(dtcolw + '\t6S\t16N\t1S\t1S\t32S\t1S\t1S')
    else:
        if first:

            # write "with keys" RDB column headings
            # WRITE (funit,'(20A)')
            #           '# //FILE TYPE="NWIS-I DAILY-VALUES" ',
            #           'EDITABLE=NO'
            funit.write(
                '# //FILE TYPE="NWIS-I DAILY-VALUES" ' +
                'EDITABLE=NO'
            )

            # write database info
            nw_rdb_dbline(funit)

            # WRITE (funit,'(20A)') outlin(1:84)
            funit.write(
                'AGENCY\tSTATION\tDD\tPARAMETER\tSTATISTIC\t' +
                'DATE\tTIME\tVALUE\tPRECISION\tREMARK\tFLAGS\tTYPE\tQA\n'
            )

            if vflag:           # verbose Excel-style format
                dtcolw = '10D'  # "mm/dd/yyyy" 10 chars
                dtlen = 3
            else:
                dtcolw = '8D'   # "yyyymmdd" 8 chars
                dtlen=2

            funit.write(
                '5S\t15S\t4S\t5S\t5S\t' + dtcolw +
                '\t6S\t16N\t1S\t1S\t32S\t1S\t1S'
            )
            first = False

    # If the DD does not exist, no sense in trying to retrieve any data
    if ddlabl != '*** NOT IN DD FILE ***':

        # get DV table names
        rtcode = 0
        if not nw_getchk_table_nm('DV', dbnum, 'R', dv_name, ldv_name):
            return 4

        if not nw_getchk_table_nm(
                'DV_DATA', dbnum, 'R', dv_data_name, ldv_data_name
        ):
            return 4

        if compdv:
            if not nw_getchk_table_nm(
                    'DV_DIFF', dbnum, 'R', dv_diff_name, ldv_diff_name
            ):
               return 4

        # Setup begin date
        if begdate == '00000000':
            if not nw_db_retr_dv_first_yr(dd_id, stat, dv_water_yr):
                return nw_get_error_number()
            # WRITE (bnwisdt,2030) dv_water_yr - 1
            #2030       FORMAT (I4.4,'1001')
            bnwisdt = "{:>4}1001".format(dv_water_yr - 1)

        # validate and load begin date into ingres FORMAT
        if not nw_cdt_ok(bnwisdt):
            return 3
        else:
            nw_dt_nwis2ing(bnwisdt,bingdt)

        # Setup end date
        if enddate == '99999999':
            if not nw_db_retr_dv_last_yr(dd_id, stat, dv_water_yr):
                return nw_get_error_number()

            # WRITE (enwisdt,2040) dv_water_yr
            #2040       FORMAT (I4.4,'0930')
            enwisdt = "{:>4}0930".format(dv_water_yr)

        # validate and load end date into ingres FORMAT
        if not nw_cdt_ok(enwisdt):
            return 3
        else:
            nw_dt_nwis2ing(enwisdt, eingdt)

        # Get DV data to a temporary file
        nwc_tmpnam(temppath)
        getfunit(1, tunit)
        # TODO:
        # OPEN (UNIT=tunit, FILE=temppath, FORM='unformatted', STATUS='unknown')
        # REWIND (tunit)

        nwc_itoa(dd_id, cdd_id, 12)
        odate = bnwisdt
        if not compdv:
            # TODO: map to AQUARIUS Web service call:
            stmt = "SELECT dvd.dv_dt, dvd.dv_va, dvd.dv_rd, " + \
                   "dvd.dv_rmk_cd, dvd.dv_type_cd, " + \
                   "dvd.data_aging_cd FROM " + \
                   dv_data_name + " dvd, " + \
                   dv_name + " dv " + \
                   "WHERE dv.dd_id = " + cdd_id + " AND " + \
                   "dv.stat_cd = '" + stat + "' AND " + \
                   "dvd.dv_id = dv.dv_id AND " +  \
                   "dvd.dv_dt >=  '" + bingdt + "' AND " +  \
                   "dvd.dv_dt <=  '" + eingdt + "' "  +  \
                   "ORDER BY dvd.dv_dt"
        else:
            # TODO: map to AQUARIUS Web service call:
            stmt = "SELECT dvd.dv_dt, dvd.dv_va, dvd.dv_rd," + \
                   "dvd.dv_rmk_cd, dvd.dv_type_cd," + \
                   "dvd.data_aging_cd FROM " + \
                   dv_data_name + " dvd, " + \
                   dv_name + " dv " + \
                   "WHERE dv.dd_id = " + cdd_id + " AND " + \
                   "dv.stat_cd = '" + stat + "' AND  " +  \
                   "dvd.dv_id = dv.dv_id AND " + \
                   "dvd.dv_type_cd = 'C' AND " + \
                   "dvd.dv_dt >=  '" + bingdt + "' AND " + \
                   "dvd.dv_dt <=  '" + eingdt + "' " +  \
                   " UNION " + \
                   "SELECT dvf.dv_dt, dvf.dv_va, dvf.dv_rd, " + \
                   "dvf.dv_rmk_cd, dvf.dv_type_cd, " + \
                   "dvf.data_aging_cd FROM " + \
                   dv_diff_name + " dvf, " + \
                   dv_name + " dv " + \
                   "WHERE dv.dd_id = " + cdd_id + " AND " + \
                   "dv.stat_cd = '" + stat + "' AND " +  \
                   "dvf.dv_id = dv.dv_id AND " + \
                   "dvf.dv_type_cd = 'C' AND " + \
                   "dvf.dv_dt >=  '" + bingdt + "' AND " + \
                   "dvf.dv_dt <=  '" + eingdt + "' " + \
                   " ORDER BY dv_dt"
         
        # EXEC SQL PREPARE pstmt FROM :stmt
        # nw_sql_error_handler('fdvrdbout', 'prepare', 'Retrieving DV data', rowcount, irc)
        if irc == 0:
            # EXEC SQL OPEN cur_stmt
            # nw_sql_error_handler('fdvrdbout', 'opencurs',
            # 'Retrieving DV data', rowcount, irc)
            if irc == 0:
                # TODO: this ESQL loop still needs to be translated:
                # DO
                #  EXEC SQL FETCH cur_stmt INTO
                #        :dv_dt, :dv_va:dv_va_null, :dv_rd:dv_rd_null,
                #        :dv_rmk_cd, :dv_type_cd, :data_aging_cd

                if nw_no_data_found(): exit
                  
                # nw_sql_error_handler('fdvrdbout', 'fetch',
                                     # 'Retrieving DV data', rowcount, irc)
                if irc != 0: exit

                nw_dt_ing2nwis(dv_dt, cdate)
                while odate < cdate:
                    # WRITE (tunit) odate, nullval, nullrd, nullrmk, 
                    #                    nulltype, nullaging
                    tunit.write(
                        odate + nullval + nullrd + nullrmk +
                        nulltype + nullaging
                    )
                    nw_dtinc(odate, 1)
                  
                # process this row
                # TODO: look up NW_NR4 in 3GL
                if dv_va_null == -1: dv_va = NW_NR4
                if dv_rd_null == -1: dv_rd = ' '
                if dv_va < NW_CR4:
                    # value is null
                    # WRITE (tunit) cdate, nullval, nullrd, dv_rmk_cd,
                    # dv_type_cd, data_aging_cd
                    tunit.write(
                        cdate + nullval + nullrd + dv_rmk_cd +
                        dv_type_cd + data_aging_cd
                    )
                else:
                    # Pick a rounding precision IF blank
                    if dv_rd == ' ':
                        if not nw_va_rget_rd(dv_va, rndary, dv_rd):
                            dv_rd = '9'
                     
                    # convert value to a character string and load it
                    if rndsup:
                        # WRITE (cval,2050) dv_va
                        # 2050                   FORMAT (E14.7)
                        cval = "{:>14.7}".format(dv_va)
                    else:
                        if not nw_va_rrnd_tx(dv_va, dv_rd, cval):
                            cval = '****'

                    s_jstrlf(cval, 20)
                    # WRITE (tunit) cdate, cval, dv_rd, dv_rmk_cd,
                    #                    dv_type_cd, data_aging_cd

                nw_dtinc(odate, 1)

                # TODO: ESQL still needs translation; see "DO" above
                # END DO
                # EXEC SQL CLOSE cur_stmt

        if irc != 0:
            return irc
         
        # EXEC SQL COMMIT

        # fill nulls to the end of the period, if the database
        # retrieval stopped short
 
        while odate <= enwisdt:
            # WRITE (tunit) odate, nullval, nullrd, nullrmk, nulltype,
            #           nullaging
            tunit.write(
                odate + nullval + nullrd + nullrmk + nulltype + nullaging
            )
            nw_dtinc(odate, 1)
        # TODO: translate
        # ENDFILE (tunit)

        # Read the temp file and write the RDB file,
        # filling in data aging where blank (did it this way
        # because the data aging routine would COMMIT the loop 

        # TODO:
        # REWIND (tunit)

        # TODO:
        #30
        # READ (tunit,END=40) cdate, cval, dv_rd, dv_rmk_cd, dv_type_cd, data_aging_cd
        if data_aging_cd == ' ':
            if not nw_db_retr_aging_for_date(dbnum, dd_id, cdate,
                                             data_aging_cd):
                return rtcode

        if data_aging_cd != 'W': rtcode = 1

        if vflag:               # build "mm/dd/yyyy"
            exdate = cdate[4:5] + '/' + cdate[6:7] + '/' + cdate[0:3]
        else:                   # copy over "yyyymmdd"
            exdate = cdate

        if addkey:
            outlin = agyin + '\t' + \
                     station + '\t' + \
                     inddid + '\t' + pcode[1:5] + '\t' + \
                     stat + '\t' + exdate
        else:
            outlin = exdate

        outlen = len(outlin)

        if cval == '**NULL**':
            outlin = outlin[0:outlen - 1] + '\t' + '\t' + \
                     '\t' + \
                     '\t' + dv_rmk_cd + '\t' + '\t' + \
                     dv_type_cd + '\t' + data_aging_cd
        else:
            outlin += '\t\t' + cval + '\t' + dv_rd + '\t' + \
                      dv_rmk_cd + '\t\t' + dv_type_cd + '\t' + \
                      data_aging_cd
            wrotedata = True

        funit.write(outlin)
        goto_30()

        #40
        # TODO:
        # CLOSE (tunit, status = 'DELETE')

    if not wrotedata and rtcode != 1: rtcode = 2

    # rtcode  =  0 - all output OK
    #          1 - some or all of data marked final
    #          2 - No non-null data in retrieval
    #          3 - error - bad begin or end date - no file written
    return rtcode

    # TODO: this is referenced above...
    #998   s_fatal ('Fatal DD file error', 'fdvrdbout', 'M', irc)

    # TODO: ...but this is not
    #999   s_fatal ('Fatal DV file error', 'fdvrdbout', 'M', irc)
