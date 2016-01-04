# File -- rdb_out.sh
#
# Purpose -- Top-level routine for outputting RDB format data
#
# Authors -- Andrew Halper <ashalper@usgs.gov>
#            Scott D. Bartholoma <sbarthol@usgs.gov>
#            Jim Cornwall <jcorn@usgs.gov>
#
rdb_out ()
{
    ctlpath=$1; shift;
    inmultiple=$1; shift;
    outpath=$1; shift;
    intyp=$1; shift;
    inrndsup=$1; shift;
    inwyflag=$1; shift;
    incflag=$1; shift;
    invflag=$1; shift;
    inhydra=$1; shift;
    inagny=$1; shift;
    instnid=$1; shift;
    inddid=$1; shift;
    inlocnu=$1; shift;
    instat=$1; shift;
    intrans=$1; shift;
    begdat=$1; shift;
    enddat=$1; shift;
    in_loc_tz_cd=$1; shift;
    titlline=$1;

# @brief Top-level routine for outputting rdb format data
# @note Module Type: INTEGER*4 FUNCTION
# @author Scott D. Bartholoma
# @date   July 17, 1997
# @param CTLPATH      (inp - C*(*)) control file path/name
# @param INMULTIPLE   (inp - C*(*)) Y/N flag to do multiple ratings
# @param OUTPATH      (inp - C*(*)) output file path/name
# @param INTYP        (inp - C*(*)) rating type
# @param INRNDSUP     (inp - C*(*)) Y/N flag for rounding-suppressed
# @param INWYFLAG     (inp - C*(*)) Y/N flag for water-year
# @param INCFLAG      (inp - C*(*)) Y/N flag for Computed DVs/Combined Datetimes (UVs)
# @param INVFLAG      (inp - C*(*)) Y/N flag for verbose dates and times
# @param INHYDRA      (inp - C*(*)) Y/N flag if handling data for Hydra
# @param INAGNY       (inp - C*(*)) agency code
# @param INSTNID      (inp - C*(*)) station number
# @param INDDID       (inp - C*(*)) DD number
# @param INLOCNU      (inp - C*(*)) Location number
# @param INSTAT       (inp - C*(*)) Statistics code
# @param INTRANS      (inp - C*(*)) UV Transport code
# @param BEGDAT       (inp - C*(*)) begin date
# @param ENDDAT       (inp - C*(*)) end date
# @param IN_LOC_TZ_CD (inp - C*(*)) time zone code
# @param TITLLINE     (inp - C*(*)) title line (text)
# @param f77_len_CTLPATH      (inp) Length of CTLPATH
# @param f77_len_INMULTIPLE   (inp) Length of INMULTIPLE
# @param f77_len_OUTPATH      (inp) Length of OUTPATH
# @param f77_len_INTYP        (inp) Length of INTYP
# @param f77_len_INRNDSUP     (inp) Length of INRNDSUP
# @param f77_len_INWYFLAG     (inp) Length of INWYFLAG
# @param f77_len_INCFLAG      (inp) Length of INCFLAG
# @param f77_len_INVFLAG      (inp) Length of INVFLAG
# @param f77_len_INHYDRA      (inp) Length of INHYDRA
# @param f77_len_INAGNY       (inp) Length of INAGNY
# @param f77_len_INSTNID      (inp) Length of INSTNID
# @param f77_len_INDDID       (inp) Length of INDDID
# @param f77_len_INLOCNU      (inp) Length of INLOCNU
# @param f77_len_INSTAT       (inp) Length of INSTAT
# @param f77_len_INTRANS      (inp) Length of INTRANS
# @param f77_len_BEGDAT       (inp) Length of BEGDAT
# @param f77_len_ENDDAT       (inp) Length of ENDDAT
# @param f77_len_IN_LOC_TZ_CD (inp) Length of IN_LOC_TZ_CD
# @param f77_len_TITLLINE     (inp) Length of TITLLINE
# @return (Integer*4) returns the error code from modules called (0 IF all OK)
# @par Purpose:
# @verbatim
# Top-level routine for outputting rdb format data
# Uses S_STRT IF needed to get missing keys for the retrieval
# @endverbatim

# **********************************************************************
# * ARGUMENT DECLARATIONS
# **********************************************************************

#     CHARACTER ctlpath*(*),
#    *          inmultiple*(*),
#    *          outpath*(*),
#    *          intyp*(*),
#    *          inrndsup*(*),
#    *          inwyflag*(*),
#    *          incflag*(*),
#    *          invflag*(*),
#    *          inhydra*(*),
#    *          inagny*(*),
#    *          instnid*(*),
#    *          inddid*(*),
#    *          inlocnu*(*),
#    *          instat*(*),
#    *          intrans*(*),
#    *          begdat*(*),
#    *          enddat*(*),
#    *          in_loc_tz_cd*(*),
#    *          titlline*(*)

# **********************************************************************
# * FUNCTION DECLARATIONS
# **********************************************************************

#     INTEGER nwf_strlen,
#    *        nwc_rdb_cfil,
#    *        nw_get_error_number,
#    *        nwc_atoi

#     LOGICAL nw_write_log_entry,
#    *        nw_key_get_zone_dst,
#    *        nw_get_dflt_tzcd,
#    *        nw_db_save_program_info,
#    *        nw_db_key_get_dd_parm_loc

# **********************************************************************
# * EXTERNAL SUBROUTINES OR FUNCTIONS
# **********************************************************************

#     EXTERNAL nwf_strlen,
#    *         nwc_rdb_cfil,
#    *         nw_write_log_entry,
#    *         nw_get_error_number,
#    *         nw_key_get_zone_dst,
#    *         nw_get_dflt_tzcd,
#    *         nw_db_save_program_info

# **********************************************************************
# * INTRINSIC FUNCTIONS
# **********************************************************************

#     INTRINSIC len

# **********************************************************************
# * INCLUDE FILES
# **********************************************************************

#     INCLUDE 'program_id.ins'
#     INCLUDE 'adaps_keys.ins'
#     INCLUDE 'user_data.ins'
#     INCLUDE 'ins.dbdata'

# **********************************************************************
# * LOCAL VARIABLE DECLARATIONS
# **********************************************************************

#     CHARACTER datatyp*2,
#    *          savetyp*2,
#    *          sopt*32,
#    *          ctlfile*128,
#    *          rdbfile*128,
#    *          rtagny*5,
#    *          sid*15,
#    *          ddid*6,
#    *          lddid*6,
#    *          parm*5,
#    *          stat*5,
#    *          transport_cd*1,
#    *          uvtyp*1,
#    *          inguvtyp*6,
#    *          mstyp*1,
#    *          mssav*1,
#    *          vttyp*1,
#    *          wltyp*1,
#    *          meth_cd*5,
#    *          pktyp*1,
#    *          qwparm*5,
#    *          qwmeth*5,
#    *          begdate*8,
#    *          enddate*8,
#    *          begdtm*14,
#    *          enddtm*14,
#    *          bctdtm*14,
#    *          ectdtm*14,
#    *          cdate*8,
#    *          ctime*6,
#    *          tz_cd*6,
#    *          loc_tz_cd*6,
#    *          local_time_fg*1

#     INTEGER loc_nu,
#    *        funit,
#    *        rdblen,
#    *        ipu,
#    *        irc,
#    *        nline,
#    *        sensor_type_id,
#    *        one,
#    *        two,
#    *        three,
#    *        iyr,
#    *        i

#     INTEGER USBUFF(91),
#    &        HOLDBUFF(91)     ! restored old code from rev 1.5

#     LOGICAL needstrt,
#    *        multiple,
#    *        rndsup,
#    *        wyflag,
#    *        cflag,
#    *        vflag,
#    *        hydra,
#    *        first,
#    *        addkey,
#    *        uvtyp_prompted

# **********************************************************************
# * INITIALIZATIONS
# **********************************************************************

    one=1; two=2; three=3;

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
#     initialize

    needstrt=false
    uvtyp_prompted=false
    funit=-1
    parm=' '
    ddid=' '
    loc_tz_cd="$in_loc_tz_cd"
    if [ loc_tz_cd = ' ' ]; then
        loc_tz_cd='LOC'
    fi

    if [ "$intrans" = ' ']; then
        transport_cd=' '
#        sensor_type_id = NW_NI4
    else
        transport_cd="$intrans"
#        CALL s_upcase (transport_cd,1)
        sensor_type_id=0
    fi

    # TODO: this is the former database connection subroutine. Check
    # if aq2rdb service is alive instead?

#     IF (.NOT. nw_write_log_entry(1)) THEN
#        CALL nw_write_error(6)
#        irc = nw_get_error_number()
#        GOTO 999
#     END IF

    # set control file path
    if [ ${#ctlpath} -gt 128 ]; then
        goto 998                # TODO
    fi
    ctlfile="$ctlpath"

    # set logical flags

    if [ "$inrndsup" = 'y' -o "$inrndsup" = 'Y' ]; then
        rndsup=true
    fi

    if [ "$inwyflag" = 'y' -o "$inwyflag" = 'Y' ]; then
        wyflag=true
    fi

    if [ "$incflag" = 'y' -o "$incflag" = 'Y' ]; then
        cflag=true
    fi

    if [ "$invflag" = 'y' -o "$invflag" = 'Y' ]; then
        vflag=true
    fi

    if [ "$inmultiple" = 'y' -o "$inmultiple" = 'Y' ]; then
        multiple=true
    fi

    if [ "$inhydra" = 'y' -o "$inhydra" = 'Y' ]; then
        hydra=true
    fi

    if [ "$ctlfile" != ' ' -a ! "$multiple" ]; then
        addkey=true
    fi

    # check for a control file

    if [ "$ctlfile" != ' ' ]; then
        # using a control file - open it
        rdb_cfil "$one" "$ctlfile" "$rtagny" "$sid" "$ddid" \
            "$stat" "$bctdtm" "$ectdtm" "$nline"
        irc=$?
#5
        if [ $irc -ne 0 ]; then
            goto 999
        fi
        s_date "$cdate" "$ctime"
#        WRITE (0,2010) cdate, ctime, ctlfile(1:nwf_strlen(ctlfile))
#2010     FORMAT (A8,1X,A6,1X,'Processing control file: ',A)

        #  get a line from the file
        first=true
        rdb_cfil "$two" "$datatyp" "$rtagny" "$sid" "$ddid" "$stat" \
            "$bctdtm" "$ectdtm" "$nline"
        irc=$?

#6        
        if [ $irc -ne 0 ]; then
            rdb_cfil "$three" "$ctlfile" "$rtagny" "$sid" "$ddid" \
                "$stat" "$bctdtm" "$ectdtm" "$nline"
            irc=$?

            # end of control file
            if [ "$first" != true ]; then
                s_mclos         # close things down and exit cleanly
                if [ $funit -ge 0 -a $funit -ne 6 ]; then
                    s_sclose $funit 'keep'
                fi
                irc=0
            fi
            goto 999
        fi

        # check data type
        if [ $hydra ]; then
            if [ "$datatyp" != 'DV' -a "$datatyp" != 'UV' -a
                 "$datatyp" != 'MS' -a "$datatyp" != 'WL' -a
                 "$datatyp" != 'QW' ]; then
                s_date "$cdate" "$ctime"
#              WRITE (0,2020) cdate, ctime, datatyp, nline
#2020           FORMAT (A8, 1X, A6, 1X, 'Invalid HYDRA data type "', A,
#    *                 '" on line ', I5, '.')
                goto 9
            fi
        else
            if [ "$datatyp" != 'DV' -a "$datatyp" != 'UV' -a
                 "$datatyp" != 'DC' -a "$datatyp" != 'SV' -a
                 "$datatyp" != 'MS' -a "$datatyp" != 'PK' ]; then
                s_date "$cdate" "$ctime"
#              WRITE (0,2021) cdate, ctime, datatyp, nline
#2021           FORMAT (A8, 1X, A6, 1X, 'Invalid data type "', A,
#    *                 '" on line ', I5, '.')
                goto 9
            fi
        fi

#        !  check for completeness
#        IF (rtagny .EQ. ' ' .OR. sid .EQ. ' ' .OR.
#    *         (datatyp .NE. 'DC'. AND. datatyp .NE. 'SV' .AND.
#    *          datatyp .NE. 'WL' .AND. datatyp .NE. 'QW' .AND.
#    *          stat .EQ. ' ') .OR.
#    *        begdtm .EQ. ' ' .OR. enddtm .EQ. ' ' .OR.
#    *         (datatyp .NE. 'MS' .AND. datatyp .NE. 'PK' .AND.
#    *          datatyp .NE. 'WL' .AND. datatyp .NE. 'QW' .AND. 
#    *          ddid .EQ. ' ')
#    *        ) THEN
#           CALL s_date (cdate, ctime)
#           WRITE (0,2030) cdate, ctime, nline
#2030        FORMAT (A8, 1X, A6, 1X, 'Incomplete row (missing items)',
#    *              ' on line ', I5, '.')
#           GOTO 9
#        END IF

#        !  zero pad stat code IF type is DV
#        IF (datatyp .EQ. 'DV') THEN
#           CALL s_jstrrt (stat, 5)
#           DO I = 1,5
#              IF (stat(i:i).EQ.' ') stat(i:i) = '0'
#           END DO
#        END IF

#        IF (first) THEN
#           savetyp = datatyp                 ! save first data type
#           mssav = stat(1:1)
#           first = .false.
#           CALL s_lgid                       ! get user id and number
#           CALL s_ndget                      ! get node data
#           CALL s_ggrp                       ! get groups (for security)
#           IF (.NOT. multiple) THEN          ! open output file
#              IF (outpath .EQ. ' ') THEN
#                 funit = 6
#              ELSE
#                 IF (len(outpath) .gt. 128) THEN
#                    irc = nwc_rdb_cfil (three, ctlfile, rtagny, sid,
#    *                           ddid, stat, bctdtm, ectdtm, nline) 
#                    GOTO 998
#                 END IF
#                 rdbfile = outpath(1:len(outpath))
#                 CALL s_file (' ', rdbfile, ' ', 'unknown', 'write',
#    *                         0, 1, ipu, funit, irc, *7)
#7                 IF (irc .NE. 0) THEN
#                    irc = nwc_rdb_cfil (three, ctlfile, rtagny, sid, 
#    *                           ddid, stat, bctdtm, ectdtm, nline)
#                    GOTO 999
#                 END IF
#              END IF
#           END IF
#        END IF

#        IF multiple not specified, all requests must
#        be the same data type as the first one

#        IF (.NOT. multiple) THEN

#           IF (datatyp .NE. savetyp) THEN
#              CALL s_date (cdate, ctime)
#              WRITE (0,2040) cdate, ctime, datatyp, savetyp, nline
#2040           FORMAT (A8, 1X, A6, 1X, 'Datatype of "', A,
#    *            '" not the same as the first request datatype of "',
#    *            A,'" on line ',I5,'.')
#              GOTO 9
#           END IF

#           IF (datatyp .EQ. 'MS' .AND. stat(1:1) .NE. mssav) THEN
#              !  can't mix types of CSG measurements
#              CALL s_date (cdate,ctime)
#              WRITE (0,2050) cdate,ctime,stat(1:1),mssav,nline
#2050           FORMAT (A8,1X,A6,1X,'Measurement type of "',A,
#    *              '" not compatible with the first ',
#    *              'measurement type of "',
#    *              A,'" on line ',I5,'.')
#              GOTO 9
#           END IF
#        END IF

#        !  convert water years to date or datetime if -w specified
#        CALL nw_rdb_fill_beg_dtm (wyflag, bctdtm, begdtm)
#        CALL nw_rdb_fill_end_dtm (wyflag, ectdtm, enddtm)

#        if multiple, open a new output file - outpath is a prefix

#        IF (multiple) THEN             ! close previously open file
#           IF (funit .GE. 0 .AND. funit .NE. 6)
#    *           CALL s_sclose (funit, 'keep')
#           !  open a new file
#           rdbfile = outpath(1:nwf_strlen(outpath)) //
#    *           '.' // datatyp // '.' //
#    *           rtagny(1:nwf_strlen(rtagny)) // '.' //
#    *           sid(1:nwf_strlen(sid))
#           rdblen = nwf_strlen(outpath)+5+
#    *           nwf_strlen(rtagny)+nwf_strlen(sid)
#           IF (datatyp .NE. 'MS' .AND. datatyp .NE. 'PK' .AND.
#    *           datatyp .NE. 'WL' .AND. datatyp .NE. 'QW') THEN
#              lddid = ddid
#              CALL s_jstrlf(lddid,4)
#              rdbfile = rdbfile(1:rdblen) // '.' //
#    *              lddid(1:nwf_strlen(lddid))
#              rdblen = rdblen + 1 + nwf_strlen(lddid)
#           END IF
#           IF (datatyp .NE. 'DC' .AND. datatyp .NE. 'SV' .AND.
#    *           datatyp .NE. 'WL' .AND. datatyp .NE. 'QW') THEN
#              rdbfile = rdbfile(1:rdblen) // '.' //
#    *              stat(1:nwf_strlen(stat))
#              rdblen = rdblen + 1 + nwf_strlen(stat)
#           END IF
#           rdbfile = rdbfile(1:rdblen) // '.' //
#    *           begdtm(1:8) // '.rdb'
#           rdblen = rdblen + 13
#           CALL s_file (' ', rdbfile, ' ', 'unknown', 'write',
#    *           0, 1, ipu, funit, irc, *8)
#8           IF (irc .NE. 0) THEN
#              CALL s_date (cdate, ctime)
#              WRITE (0,2060) cdate, ctime, irc, nline,
#    *                        rdbfile(1:rdblen)
#2060           FORMAT (A8, 1X, A6, 1X, 'Error ', I5,
#    *                 ' opening output file for line ',
#    *                 I5, '.', /, 16X, A)
#              irc = nwc_rdb_cfil (three, ctlfile, rtagny, sid, ddid, 
#    *                             stat,bctdtm, ectdtm, nline)
#              CALL s_mclos
#              GOTO 999
#           END IF
#           CALL s_date (cdate, ctime)
#           WRITE (0,2070) cdate, ctime, rdbfile(1:rdblen)
#2070        FORMAT (A8, 1X, A6, 1X, 'Writing file ', A)
#        END IF

#        check DD for a P in column 1 - indicated parm code for PR DD search

#        IF (ddid(1:1) .EQ. 'p' .or. ddid(1:1) .EQ. 'P') THEN
#           parm = ddid(2:6)
#           CALL s_jstrrt (parm, 5)
#           DO I = 1,5
#              IF (parm(i:i) .EQ. ' ') parm(i:i) = '0'
#           END DO
#           CALL nwf_get_prdd (rtdbnum, rtagny, sid, parm, ddid, irc)
#           IF (irc .NE. 0) THEN
#              CALL s_date (cdate, ctime)
#              WRITE (0,2035) cdate, ctime, rtagny, sid, parm, nline
#2035           FORMAT (A8, 1X, A6, 1X, 'No PRIMARY DD for station "', 
#    *                 A5, A15, '", parm "', A5, '" on line ', I5, '.')
#              GOTO 9
#           END IF
#        ELSE         ! right justify DDID to 4 characters
#           IF (datatyp .NE. 'MS' .AND. datatyp .NE. 'PK' .AND.
#    *          datatyp .NE. 'WL' .AND. datatyp .NE. 'QW') THEN
#              CALL s_jstrrt (ddid,4)
#           END IF
#        END IF

#        !  process the request
#        IF (datatyp .EQ. 'DV') THEN

#           CALL fdvrdbout (funit, .false., rndsup, addkey, vflag, 
#    *                      cflag, rtagny, sid, ddid, stat, 
#    *                      begdtm, enddtm, irc)

#        ELSE IF (datatyp .EQ. 'UV') THEN

#           uvtyp = stat(1:1)
#           IF (uvtyp .NE. 'M' .AND. uvtyp .NE. 'N' .AND. uvtyp .NE. 'E'
#    *          .AND. uvtyp .NE. 'R' .AND. uvtyp .NE. 'S' .AND. 
#    *          uvtyp .NE. 'C') THEN
#              CALL s_date (cdate, ctime)
#              WRITE (0,2080) cdate, ctime, uvtyp, nline
#2080           FORMAT (A8, 1X, A6, 1X, 'Invalid unit-values type "', 
#    *                 A1, '" on line ', I5,'.')
#           ELSE
#              IF (uvtyp .EQ. 'M') inguvtyp = 'meas'
#              IF (uvtyp .EQ. 'N') inguvtyp = 'msar'
#              IF (uvtyp .EQ. 'E') inguvtyp = 'edit'
#              IF (uvtyp .EQ. 'R') inguvtyp = 'corr'
#              IF (uvtyp .EQ. 'S') inguvtyp = 'shift'
#              IF (uvtyp .EQ. 'C') inguvtyp = 'da'
#              CALL fuvrdbout (funit, .false., rtdbnum, rndsup, cflag,
#    *                         vflag, addkey, rtagny, sid, ddid,  
#    *                         inguvtyp, sensor_type_id, transport_cd,
#    *                          begdtm, enddtm, loc_tz_cd, irc)
#           END IF

#        ELSE IF (datatyp .EQ. 'MS') THEN

#           mstyp = stat(1:1)

#           Only standard meas types allowed when working from a control file
#           Pseudo-UV Types 1 through 3 are only good from the command line or in hydra mode

#           IF (mstyp .NE. 'C' .AND. mstyp .NE. 'M' .AND.
#    *          mstyp .NE. 'D' .AND. mstyp .NE. 'G') THEN 
#              CALL s_date (cdate, ctime)
#              WRITE (0,2090) cdate, ctime, mstyp, nline
#2090           FORMAT (A8, 1X, A6, 1X,
#    *                 'Invalid measurement file type "', A1,
#    *                 '" on line ', I5, '.')
#           ELSE

#              CALL fmsrdbout (funit, rtdbnum, rndsup, addkey, cflag,
#    *                         vflag, rtagny, sid, mstyp, begdtm, 
#    *                         enddtm, irc)

#           END IF
#           
#        ELSE IF (datatyp .EQ. 'PK') THEN
#                
#           pktyp = stat(1:1)
#           IF (pktyp .NE. 'F' .AND. pktyp .NE. 'P' .AND.
#    &          pktyp .NE. 'B') THEN
#              CALL s_date (cdate, ctime)
#              WRITE (0,2100) cdate, ctime, pktyp, nline
#2100           FORMAT (A8,1X,A6,1X,'Invalid peak flow file type "',A1,
#    *              '" on line ',I5,'.')
#           ELSE

#              CALL fpkrdbout (funit, rndsup, addkey, cflag, vflag, 
#    *                         rtagny, sid, pktyp, begdtm, enddtm, irc)

#           END IF
#           
#        ELSE IF (datatyp .EQ. 'DC') THEN

#           CALL fdcrdbout (funit, rndsup, addkey, cflag, vflag, 
#    *                      rtagny, sid, ddid, begdtm, enddtm, 
#    *                      loc_tz_cd, irc)
#           
#        ELSE IF (datatyp .EQ. 'SV') THEN
#                
#           CALL fsvrdbout (funit, rndsup, addkey, cflag, vflag, 
#    *                      rtagny, sid, ddid, begdtm, enddtm, 
#    *                      loc_tz_cd, irc)
#           
#        END IF

#        !  get next line from control file
#9        irc = nwc_rdb_cfil (two, datatyp, rtagny, sid, ddid, stat,
#    *                       bctdtm, ectdtm, nline)
#        GOTO 6

#     ELSE       ! Not a control file

#        sopt = '10000000000000000000000000000000'      ! init control argument
#        if (len(intyp).gt.2) then
#           datatyp = intyp(1:2)     
#        else
#           datatyp = intyp(1:len(intyp))
#        end if
#        
#                      ! check data type
#        CALL s_upcase (datatyp,2)

#        IF (hydra) THEN
#           needstrt = .true.
#           sopt(8:8) = '1'
#           sopt(12:12) = '2'
#           IF (datatyp .NE. 'DV' .AND. datatyp .NE. 'UV' .AND.
#    *          datatyp .NE. 'MS' .AND. datatyp .NE. 'WL' .AND.
#    *          datatyp .NE. 'QW')   datatyp = 'UV'

#           ! convert dates to 8 characters
#           CALL nw_rdb_fill_beg_date (wyflag,begdat,begdate)
#           CALL nw_rdb_fill_end_date (wyflag,enddat,enddate)

#           ! convert date/times to 14 characters
#           CALL nw_rdb_fill_beg_dtm (wyflag,begdat,begdtm)
#           CALL nw_rdb_fill_end_dtm (wyflag,enddat,enddtm)

#        ELSE

#           IF (cflag) THEN   ! Data type VT is pseudo-UV, no combining of date time possible

#              IF (datatyp .NE. 'DV' .AND. datatyp .NE. 'UV' .AND.
#    *             datatyp .NE. 'DC' .AND. datatyp .NE. 'SV' .AND.
#    *             datatyp .NE. 'MS' .AND. datatyp .NE. 'PK' .AND. 
#    *             datatyp .NE. 'WL' .AND. datatyp .NE. 'QW') THEN
#   
#11                datatyp = ' '
#                 PRINT *,'Valid data types are:'
#                 PRINT *,'   DV - Daily Values'
#                 PRINT *,'   UV - Unit Values'
#                 PRINT *,'   MS - Discharge Measurements'
#                 PRINT *,'   PK - Peak Flows'
#                 PRINT *,'   DC - Data Corrections'
#                 PRINT *,'   SV - Variable Shift'
#                 PRINT *,'   WL - Water Levels from GWSI'
#                 PRINT *,'   QW - QW Data From QWDATA'
#                 CALL s_qryc ('Enter desired data type: ',' ',0,0,2,2,
#    *                         datatyp,*11)
#                 CALL s_upcase (datatyp,2)
#                 IF (datatyp .NE. 'DV' .AND. datatyp .NE. 'UV' .AND.
#    *                datatyp .NE. 'DC' .AND. datatyp .NE. 'SV' .AND.
#    *                datatyp .NE. 'MS' .AND. datatyp .NE. 'PK' .AND. 
#    *                datatyp .NE. 'WL' .AND. datatyp .NE. 'QW') 
#    *                       CALL s_bada ('Please answer ' //
#    *                       '"DV", "UV", "MS", "PK", "DC", "ST",' // 
#    *                       ' "SV", "WL" or "QW".', *11) 

#              END IF

#           ELSE

#              IF (datatyp .NE. 'DV' .AND. datatyp .NE. 'UV' .AND.
#    *             datatyp .NE. 'DC' .AND. datatyp .NE. 'SV' .AND.
#    *             datatyp .NE. 'MS' .AND. datatyp .NE. 'VT' .AND.
#    *             datatyp .NE. 'PK' .AND. datatyp .NE. 'WL' .AND.
#    *             datatyp .NE. 'QW') THEN

#12                datatyp = ' '
#                 PRINT *,'Valid data types are:'
#                 PRINT *,'   DV - Daily Values'
#                 PRINT *,'   UV - Unit Values'
#                 PRINT *,'   MS - Discharge Measurements'
#                 PRINT *,'   VT - Site Visit Readings'
#                 PRINT *,'   PK - Peak Flows'
#                 PRINT *,'   DC - Data Corrections'
#                 PRINT *,'   SV - Variable Shift'
#                 PRINT *,'   WL - Water Levels from GWSI'
#                 PRINT *,'   QW - QW Data From QWDATA'
#                 CALL s_qryc ('Enter desired data type: ',' ',0,0,2,2,
#    *                 datatyp,*12)
#                 CALL s_upcase (datatyp,2)
#                 IF (datatyp .NE. 'DV' .AND. datatyp .NE. 'UV' .AND.
#    *                datatyp .NE. 'DC' .AND. datatyp .NE. 'SV' .AND.
#    *                datatyp .NE. 'MS' .AND. datatyp .NE. 'VT' .AND.
#    *                datatyp .NE. 'PK' .AND. datatyp .NE. 'WL' .AND.
#    *                datatyp .NE. 'QW')
#    *                 CALL s_bada ('Please answer ' //
#    *                 '"DV", "UV", "MS", "VT", "PK", "DC", "ST",' // 
#    *                 ' "SV", "WL" or "QW".',
#    *                 *12)

#              END IF

#           END IF

#        END IF

#        !  convert agency to 5 characters - default to USGS
#        IF (inagny(1:len(inagny)) .EQ. ' ') THEN
#           rtagny = 'USGS'
#        ELSE
#           IF (len(inagny) .GT. 5) THEN
#              rtagny = inagny(1:5)
#           ELSE
#              rtagny = inagny(1:len(inagny))
#           END IF
#           CALL s_jstrlf (rtagny,5)
#        END IF

#        !  convert station to 15 characters
#        IF (instnid(1:len(instnid)) .EQ. ' ') THEN
#           needstrt = .true.
#           IF (datatyp .EQ. 'MS' .OR. datatyp .EQ. 'PK' .OR.
#    *          datatyp .EQ. 'WL' .OR. datatyp .EQ. 'QW') THEN
#              sopt(5:5) = '1'
#           ELSE
#              sopt(5:5) = '2'
#           END IF
#        ELSE
#           IF (len(instnid) .GT. 15) THEN
#              sid = instnid(1:15)
#           ELSE
#              sid = instnid(1:len(instnid))
#           END IF
#           CALL s_jstrlf (sid, 15)
#        END IF

#        DD is ignored for data types MS, PR, WL, and QW

#        IF (datatyp .NE. 'MS' .AND. datatyp .NE. 'PK' .AND.
#    *       datatyp .NE. 'WL' .AND. datatyp .NE. 'QW') THEN

#           ! If type is VT, DDID is only needed IF parm and loc number are not specified
#           IF ((datatyp .NE. 'VT' .AND. inddid(1:len(inddid)) .EQ. ' ')
#    *              .OR.
#    *          (datatyp .EQ. 'VT' .AND. inddid(1:len(inddid)) .EQ. ' ' 
#    *            .AND. (inddid(1:1) .NE. 'P' .OR. inlocnu .EQ. ' ') )
#    *                                                   ) THEN
#              needstrt = .true.
#              sopt(5:5) = '2'

#           ELSE

#              ! If ddid starts with "P", it is a parameter code, fill to 5 digits
#              IF (inddid(1:1) .EQ. 'p' .OR. inddid(1:1) .EQ. 'P') THEN
#                 IF (len(inddid) .GT. 6) THEN
#                    parm = inddid(2:6)
#                 ELSE
#                    parm = inddid(2:len(inddid))
#                 END IF
#                 CALL s_jstrrt (parm, 5)
#                 DO I = 1,5
#                    IF (parm(i:i) .EQ. ' ') parm(i:i) = '0'
#                 END DO
#              ELSE
#                 parm = ' '
#                 !  convert ddid to 4 characters
#                 IF (len(inddid) .gt. 4) THEN
#                    ddid = inddid(1:4)
#                 ELSE
#                    ddid = inddid(1:len(inddid))
#                 END IF
#                 CALL s_jstrrt (ddid, 4)

#              END IF

#           END IF

#        END IF

#        further processing depends on data type

#        IF (datatyp .EQ. 'DV') THEN       ! convert stat to 5 characters
#           IF (instat(1:len(instat)).EQ.' ') THEN
#              needstrt = .true.
#              sopt(8:8) = '1'
#           ELSE
#              IF (len(instat).gt.5) THEN
#                 stat = instat(1:5)
#              ELSE
#                 stat = instat(1:len(instat))
#              END IF
#              CALL s_jstrrt (stat,5)
#              DO I = 1,5
#                 IF (stat(i:i) .EQ. ' ') stat(i:i) = '0'
#              END DO
#           END IF
#        END IF

#        IF (datatyp .EQ. 'DV' .OR. datatyp .EQ. 'DC' .OR.
#    *       datatyp .EQ. 'SV' .OR. datatyp .EQ. 'PK') THEN

#           !  convert dates to 8 characters
#           IF (begdat(1:len(begdat)) .EQ. ' '.or.
#    *          enddat(1:len(enddat)) .EQ. ' ') THEN
#              needstrt = .true.
#              IF (wyflag) THEN
#                 sopt(9:9) = '4'
#              ELSE
#                 sopt(10:10) = '3'
#              END IF
#           ELSE
#              CALL nw_rdb_fill_beg_date (wyflag, begdat, begdate)
#              CALL nw_rdb_fill_end_date (wyflag, enddat, enddate)
#           END IF

#        END IF

#        IF (datatyp .EQ. 'UV') THEN

#           IF (.NOT. hydra) THEN        ! get UV type
#              uvtyp = instat(1:1)
#              IF (uvtyp .EQ. 'm') uvtyp = 'M'
#              IF (uvtyp .EQ. 'n') uvtyp = 'N'
#              IF (uvtyp .EQ. 'e') uvtyp = 'E'
#              IF (uvtyp .EQ. 'r') uvtyp = 'R'
#              IF (uvtyp .EQ. 's') uvtyp = 'S'
#              IF (uvtyp .EQ. 'c') uvtyp = 'C'
#              IF (uvtyp .NE. 'M' .AND. uvtyp .NE. 'N' .AND. 
#    *             uvtyp .NE. 'E' .AND. uvtyp .NE. 'R' .AND. 
#    *             uvtyp .NE. 'S' .AND. uvtyp .NE. 'C') THEN
#                 uvtyp_prompted = .TRUE.
#50                uvtyp = ' '
#                 CALL s_qryc (
#    *                 'Unit values type (M, N, E, R, S, or C): ',
#    *                 ' ', 0, 0, 1, 1, uvtyp, *50)
#                 IF (uvtyp .EQ. 'm') uvtyp = 'M'
#                 IF (uvtyp .EQ. 'n') uvtyp = 'N'
#                 IF (uvtyp .EQ. 'e') uvtyp = 'E'
#                 IF (uvtyp .EQ. 'r') uvtyp = 'R'
#                 IF (uvtyp .EQ. 's') uvtyp = 'S'
#                 IF (uvtyp .EQ. 'c') uvtyp = 'C'
#                 IF (uvtyp .NE. 'M' .AND. uvtyp .NE. 'N' .AND.
#    *                uvtyp .NE. 'E' .AND. uvtyp .NE. 'R' .AND.
#    *                uvtyp .NE. 'S' .AND. uvtyp .NE. 'C') CALL s_bada (
#    *                 'Please answer "M", "N", "E", "R", "S", or "C".',
#    *                 *50)
#              END IF
#           END IF

#           !  convert date/times to 14 characters
#           IF (begdat(1:len(begdat)) .EQ. ' ' .or.
#    *          enddat(1:len(enddat)) .EQ. ' ') THEN
#              needstrt = .true.
#              IF (wyflag) THEN
#                 sopt(9:9) = '4'
#              ELSE
#                 sopt(10:10) = '3'
#              END IF
#           ELSE
#              CALL nw_rdb_fill_beg_dtm (wyflag, begdat, begdtm)
#              CALL nw_rdb_fill_end_dtm (wyflag, enddat, enddtm)
#           END IF

#        END IF

#        If hydra mode for UV data, set time zone code that in effect
#        for the first date for this station

#        IF (hydra .AND. datatyp .NE. 'UV') THEN
#           IF (.NOT. nw_key_get_zone_dst (rtdbnum, rtagny, sid,
#    *                                    tz_cd, local_time_fg)) THEN
#              loc_tz_cd = 'UTC'          ! default to UTC
#           ELSE
#              IF (.NOT. nw_get_dflt_tzcd (tz_cd, local_time_fg,
#    *                                     begdtm(1:8), loc_tz_cd)) THEN
#                 loc_tz_cd = 'UTC'       ! default to UTC
#              END IF
#           END IF
#        END IF

#        IF (datatyp .EQ. 'MS') THEN       ! get MS type
#           mstyp = instat(1:1)
#           CALL nwc_upcase (mstyp)

#           IF (mstyp .NE. 'C' .AND. mstyp .NE. 'M' .AND. 
#    *          mstyp .NE. 'D' .AND. mstyp .NE. 'G' .AND.
#    *          mstyp .NE. '1' .AND. mstyp .NE. '2' .AND. 
#    *          mstyp .NE. '3') THEN
#45             mstyp = ' '

#              PRINT 1234
#1234           FORMAT (/,'Measurement file retrieval type -',/,
#    *              '  C - Crest Stage Gage measurements,',/,
#    *              '  M - Discharge Measurements,',/,
#    *              '  D - Detailed Discharge Measurements,',/,
#    *              '  G - Gage Inspections,',/,
#    *              '  1 - Pseudo UV, measurement discharge,',/,
#    *              '  2 - Pseudo UV, measurement stage, or',/,
#    *              '  3 - Pseudo UV, mean index velocity')

#              CALL s_qryc ('|Enter C, M, D, G, or 1 to 3: ',
#    *                      ' ', 0, 0, 1, 1, mstyp, *45)
#              CALL nwc_upcase (mstyp)
#              IF (mstyp .NE. 'C' .AND. mstyp .NE. 'M' .AND. 
#    *             mstyp .NE. 'D' .AND. mstyp .NE. 'G' .AND. 
#    *             mstyp .NE. '1' .AND. mstyp .NE. '2' .AND. 
#    *             mstyp .NE. '3')      CALL s_bada (
#    *                 'Please answer "C", "M", "G", or "1" to "3".',
#    *                 *45)
#           END IF

#           IF (begdat(1:len(begdat)) .EQ. ' ' .OR.
#    *          enddat(1:len(enddat)) .EQ. ' ') THEN
#              needstrt = .true.
#              IF (wyflag) THEN
#                 sopt(9:9) = '4'
#              ELSE
#                 sopt(10:10) = '3'
#              END IF

#           ELSE

#              IF (mstyp .GE. '1' .AND. mstyp .LE. '3') THEN
#                 !  doing pseudo-uv, convert date/times to 14 characters
#                 CALL nw_rdb_fill_beg_dtm (wyflag, begdat, begdtm)
#                 CALL nw_rdb_fill_end_dtm (wyflag, enddat, enddtm)
#              ELSE
#                 !  convert dates to 8 characters
#                 CALL nw_rdb_fill_beg_date (wyflag, begdat, begdate)
#                 CALL nw_rdb_fill_end_date (wyflag, enddat, enddate)
#              END IF
#           END IF
#        END IF

#        IF (datatyp .EQ. 'VT') THEN     ! get VT type
#           vttyp = instat(1:1)
#           CALL nwc_upcase (vttyp)

#           IF (vttyp .NE. 'P' .AND. vttyp .NE. 'R' .AND.
#    *          vttyp .NE. 'A' .AND. vttyp .NE. 'M' .AND.
#    *          vttyp .NE. 'F') THEN

#55             vttyp = 'A'

#              PRINT 1235
#1235           FORMAT (/,'SiteVisit Pseudo UV readings ',
#    *              'retrieval type -',/,
#    *              '  P - Retrieve sensor insp. ',
#    *              'primary reference readings',/,
#    *              '  R - Retrieve sensor insp. ',
#    *              'primary recorder readings',/,
#    *              '  A - Retrieve sensor insp. ',
#    *              'all readings',/,
#    *              '  M - Retrieve QW monitor readings',/,
#    *              '  F - Retrieve QW field meter readings')

#              CALL s_qryc ('|Enter P, R, A, M, or F (<CR> = A):',
#    *                      ' ', 0, 1, 1, 1, vttyp, *55)
#              CALL nwc_upcase (vttyp)
#              IF (vttyp .NE. 'P' .AND. vttyp .NE. 'R' .AND.
#    *             vttyp .NE. 'A' .AND. vttyp .NE. 'M' .AND.
#    *             vttyp .NE. 'F')    CALL s_bada (
#    *                     'Please answer "P", "R", "A", "M" or "F".',
#    *                     *55)

#           END IF

#           !  See if we have the date range
#           IF (begdat(1:len(begdat)) .EQ. ' ' .OR.
#    *          enddat(1:len(enddat)) .EQ. ' ') THEN
#              needstrt = .TRUE.
#              IF (wyflag) THEN
#                 sopt(9:9) = '4'
#              ELSE
#                 sopt(10:10) = '3'
#              END IF
#           END IF

#           !  Doing pseudo-uv, convert date/times to 14 characters
#           CALL nw_rdb_fill_beg_dtm (wyflag, begdat, begdtm)
#           CALL nw_rdb_fill_end_dtm (wyflag, enddat, enddtm)

#        END IF

#        IF (datatyp .EQ. 'PK') THEN       ! get pk type

#           pktyp = instat(1:1)
#           IF (pktyp .EQ. 'f') pktyp = 'F'
#           IF (pktyp .EQ. 'p') pktyp = 'P'
#           IF (pktyp .EQ. 'b') pktyp = 'B'

#           IF (pktyp .NE. 'F' .AND. pktyp .NE. 'P' .AND. 
#    *          pktyp .NE. 'B') THEN
#46             pktyp = ' '
#              CALL s_qryc ('Peak flow file retrieval type -' //
#    *              '|Full peaks only (F),' //
#    *              '|Partial peaks only (P),' //
#    *              '|Both Full and Partial peaks (B) - ' //
#    *              '|Please enter F, P, or B: ',' ',0,0,1,1,
#    *              pktyp, *46)
#              IF (pktyp .EQ. 'f') pktyp = 'F'
#              IF (pktyp .EQ. 'p') pktyp = 'P'
#              IF (pktyp .EQ. 'b') pktyp = 'B'
#              IF (pktyp .NE. 'F' .AND. pktyp .NE. 'P' .AND. 
#    *             pktyp .NE. 'B')    CALL s_bada (
#    *                     'Please answer "F", "P",  or "B".', *46)
#           END IF

#        END IF

#        IF (datatyp .EQ. 'WL') THEN
#          wltyp = instat(1:1)
#           IF (.NOT. (wltyp .GE. '1' .AND. wltyp .LE. '3')) wltyp = ' '
#           !  convert date/times to 14 characters
#           IF (begdat(1:len(begdat)) .EQ. ' ' .OR.
#    *          enddat(1:len(enddat)) .EQ. ' ') THEN
#              needstrt = .true.
#              sopt(5:5) = '1'
#              IF (wyflag) THEN
#                 sopt(9:9) = '4'
#              ELSE
#                 sopt(10:10) = '3'
#              END IF
#           ELSE
#              CALL nw_rdb_fill_beg_dtm (wyflag, begdat, begdtm)
#              CALL nw_rdb_fill_end_dtm (wyflag, enddat, enddtm)
#           END IF
#        END IF

#        IF (datatyp .EQ. 'QW') THEN
#           qwparm = ' '
#           IF (len(inddid) .GE. 2) THEN
#              qwparm = inddid(2:len(inddid))
#           END IF
#           qwmeth = instat(1:len(instat))
#           !  convert date/times to 14 characters
#           IF (begdat(1:len(begdat)) .EQ. ' ' .OR.
#    *          enddat(1:len(enddat)) .EQ. ' ') THEN
#              needstrt = .true.
#              sopt(5:5) = '1'
#              IF (wyflag) THEN
#                 sopt(9:9) = '4'
#              ELSE
#                 sopt(10:10) = '3'
#              END IF
#           ELSE
#              CALL nw_rdb_fill_beg_dtm (wyflag, begdat, begdtm)
#              CALL nw_rdb_fill_end_dtm (wyflag, enddat, enddtm)
#           END IF
#        END IF

#        IF (NEEDSTRT) THEN                      ! call s_strt if needed
#           CALL S_MDUS (NW_OPRW, IRC, *998)     ! get USER info 
#           IF (IRC .NE. 0) THEN
#              WRITE (0,2110)
#2110           FORMAT (/,'Unable to open ADAPS User file - Aborting.',/)
#              GO TO 998
#           END IF
#           CALL S_LGID                          ! get user info 
#           CALL S_MDUS (NW_READ,IRC,*998)
#           IF (IRC .EQ. 0) THEN                 ! save the user info
#              DO I  =  1, 91
#                 HOLDBUFF(I)  =  USBUFF(I)
#              END DO

#              IF (SOPT(5:5) .EQ. '1' .OR. SOPT(5:5) .EQ. '2') THEN
#                 AGENCY  =  RTAGNY
#                 IF (INSTNID(1:LEN(INSTNID)) .NE. ' ') STNID  =  SID
#              END IF
#              CALL S_MDUS (NW_UPDT, IRC, *998)  ! save modified user info
#           END IF

#           ! call start routine
#           prgid = 'NWTS2RDB'
#           IF (titlline  .EQ. ' ') THEN
#              prgdes = 'TIME-SERIES TO RDB OUTPUT'
#           ELSE
#              IF (nwf_strlen(titlline) .GT. 80) THEN
#                 prgdes = titlline(1:80)
#              ELSE
#                 prgdes = titlline(1:nwf_strlen(titlline))
#              END IF
#           END IF
#           rdonly = 1
#123         CALL s_strt (sopt, *998)
#           sopt(1:1) = '2'

#           IF (sopt(5:5) .EQ. '1' .OR. sopt(5:5) .EQ. '2') THEN
#              rtagny = agency              ! get agency
#              sid = stnid                  ! get stn ID
#              IF (sopt(5:5) .EQ. '2') THEN
#                 ddid = usddid             ! and DD number
#              END IF
#           END IF

#           IF (ddid .EQ. ' ') THEN
#              IF (parm .NE. ' ' .AND. datatyp .NE. 'VT') THEN
#                 CALL nwf_get_prdd (rtdbnum, rtagny, sid, parm, ddid,
#    &                               irc)
#                 IF (irc .NE. 0) THEN
#                    WRITE (0,2120) rtagny, sid, parm
#                    GOTO 999
#                 END IF
#              END IF
#           END IF

#           !  stat code
#           IF (sopt(8:8) .EQ. '1') stat = statcd

#           !  data type
#           IF (sopt(12:12) .EQ. '2') THEN
#              uvtyp_prompted = .TRUE.
#              IF (usdtyp .EQ. 'D') THEN
#                 datatyp = 'DV'
#                 cflag = .FALSE.
#              ELSE IF (usdtyp .EQ. 'V') THEN
#                 datatyp = 'DV'
#                 cflag = .TRUE.
#              ELSE IF (usdtyp .EQ. 'U') THEN
#                 datatyp = 'UV'
#                 uvtyp = 'M'
#              ELSE IF (usdtyp .EQ. 'N') THEN
#                 datatyp = 'UV'
#                 uvtyp = 'N'
#              ELSE IF (usdtyp .EQ. 'E') THEN
#                 datatyp = 'UV'
#                 uvtyp = 'E'
#              ELSE IF (usdtyp .EQ. 'R') THEN
#                 datatyp = 'UV'
#                 uvtyp = 'R'
#              ELSE IF (usdtyp .EQ. 'S') THEN
#                 datatyp = 'UV'
#                 uvtyp = 'S'
#              ELSE IF (usdtyp .EQ. 'C') THEN
#                 datatyp = 'UV'
#                 uvtyp = 'C'
#              ELSE IF (usdtyp .EQ. 'M') THEN
#                 datatyp = 'MS'
#              ELSE IF (usdtyp .EQ. 'X') THEN
#                 datatyp = 'VT'
#              ELSE IF (usdtyp .EQ. 'L') THEN
#                 datatyp = 'WL'
#              ELSE IF (usdtyp .EQ. 'Q') THEN
#                 datatyp = 'QW'
#              END IF
#           END IF

#           !  date range for water years
#           IF (sopt(9:9) .EQ. '4') THEN
#              IF (usyear .EQ. '9999') THEN
#                 begdtm = '00000000000000'
#                 begdate = '00000000'
#              ELSE
#                 READ (usyear,1010) iyr
#1010              FORMAT (I4)
#                 WRITE (usdate,2140) iyr-1,10,01
#2140              FORMAT (I4.4,2I2.2)
#                 begdtm = usdate // '000000'
#                 begdate = usdate
#              END IF
#              IF (ueyear .EQ. '9999') THEN
#                 enddtm = '99999999999999'
#                 enddate = '99999999'
#              ELSE
#                 READ (ueyear,1010) iyr
#                 WRITE (uedate,2140) iyr,9,30
#                 enddtm = uedate // '235959'
#                 enddate = uedate
#              END IF
#           END IF

#           !  date range
#           IF (sopt(10:10) .EQ. '3') THEN
#                 begdate = usdate
#                 enddate = uedate
#                 begdtm = usdate // '000000'
#                 IF (uedate .EQ. '99999999') THEN
#                    enddtm = '99999999999999'
#                 ELSE
#                    enddtm = uedate // '235959'
#                 END IF
#           END IF

#           !  Restore contents of user buffer
#           IF (IRC .EQ. 0) THEN
#              DO I  =  1, 91
#                 USBUFF(I)  =  HOLDBUFF(I)
#              END DO
#              CALL S_MDUS (NW_UPDT, IRC, *998)
#           ENDIF

#        ELSE

#           CALL s_lgid                 ! get user id and number
#           CALL s_ndget                ! get node data
#           CALL s_ggrp                 ! get groups (for security)
#           CALL sen_dbop (rtdbnum)     ! open Midas files
#           !  count program (counted by S_STRT above if needed)
#           IF (.NOT. nw_db_save_program_info ('NWTS2RDB')) THEN
#              CONTINUE       ! ignore errors, we don't care if not counted
#           END IF
#           !  get PRIMARY DD that goes with parm if parm supplied
#           IF (parm .NE. ' ' .AND. datatyp .NE. 'VT') THEN
#              CALL nwf_get_prdd (rtdbnum, rtagny, sid, parm, ddid, irc)
#              IF (irc .NE. 0) THEN
#                 WRITE (0,2120) rtagny, sid, parm
#2120              FORMAT (/,'No PRIMARY DD for station "',A5,A15,
#    *                 '", parm "',A5,'".  Aborting.',/)
#                 GOTO 999
#              END IF
#           END IF

#        END IF

#        !  retrieving measured uvs and transport_cd not supplied, prompt for it
#        IF (uvtyp_prompted. AND. datatyp .EQ. 'UV' .AND.
#    *        (uvtyp .EQ. 'M' .OR. uvtyp .EQ. 'N') .AND.
#    *        transport_cd .EQ. ' ') THEN
#           CALL nw_query_meas_uv_type (rtagny, sid, ddid, begdtm,
#    &                                  enddtm, loc_tz_cd, transport_cd,
#    &                                  sensor_type_id, *998)
#           IF (transport_cd .EQ. ' ') THEN
#              WRITE (0,2150) rtagny, sid, ddid
#2150           FORMAT (/,'No MEASURED UV data for station "',A5,A15,
#    *              '", DD "',A4,'".  Aborting.',/)
#              GOTO 999
#           END IF
#        END IF

#        !  Open output file
#        IF (outpath .EQ. ' ') THEN
#           funit = 6
#        ELSE
#           IF (len(outpath) .gt. 128) GOTO 998
#           rdbfile = outpath(1:len(outpath))
#           CALL s_file (' ', rdbfile, ' ', 'unknown', 'write', 0, 1,
#    *                  ipu, funit, irc, *90)
#90          IF (irc .NE. 0) THEN
#              WRITE (0,2130) rdbfile(1:nwf_strlen(rdbfile))
#2130           FORMAT (/,'Error ',I5,' opening output file:',/,3X,A,/)
#              GOTO 999
#           END IF
#        END IF

#        !  get data and output to files

#        IF (datatyp .EQ. 'DV') THEN

#           CALL fdvrdbout (funit, .false., rndsup, addkey, vflag,
#    &                      cflag, rtagny, sid, ddid, stat, 
#    *                      begdate, enddate, irc)

#        ELSE IF (datatyp .EQ. 'UV') THEN

#           IF (uvtyp .EQ. 'M') inguvtyp = 'meas'
#           IF (uvtyp .EQ. 'N') inguvtyp = 'msar'
#           IF (uvtyp .EQ. 'E') inguvtyp = 'edit'
#           IF (uvtyp .EQ. 'R') inguvtyp = 'corr'
#           IF (uvtyp .EQ. 'S') inguvtyp = 'shift'
#           IF (uvtyp .EQ. 'C') inguvtyp = 'da'

#           CALL fuvrdbout (funit, .false., rtdbnum, rndsup, cflag,
#    *                      vflag, addkey, rtagny, sid, ddid, inguvtyp, 
#    *                      sensor_type_id, transport_cd, begdtm, 
#    *                      enddtm, loc_tz_cd, irc)

#        ELSE IF (datatyp .EQ. 'MS') THEN

#           IF (hydra .OR. (mstyp .GE. '1' .AND. mstyp .LE. '3')) THEN
#              IF (hydra) mstyp = ' '
#              CALL fmsrdbout_hydra (funit, rndsup, rtagny, sid,
#    *                               begdtm, enddtm, loc_tz_cd, 
#    *                               mstyp, irc)
#           ELSE
#              CALL fmsrdbout (funit, rtdbnum, rndsup, addkey, cflag,
#    *                         vflag, rtagny, sid, mstyp, begdate,  
#    *                         enddate, irc)
#           END IF

#        ELSE IF (datatyp .EQ. 'VT') THEN

#           !  Get parm and loc number from DD IF not specified in arguments
#           IF (inddid(1:1) .NE. 'P' .OR. inlocnu .EQ. ' ') THEN
#              IF (.NOT. nw_db_key_get_dd_parm_loc (rtdbnum, rtagny, 
#    *                                              sid, ddid, parm,
#    *                                              loc_nu)) GOTO 997
#           ELSE
#              loc_nu = nwc_atoi(inlocnu)
#           END IF

#           CALL fvtrdbout_hydra (funit, rndsup, rtagny, sid, parm,
#    *                            loc_nu, begdtm, enddtm, loc_tz_cd,
#    *                            vttyp, irc)

#        ELSE IF (datatyp .EQ. 'PK') THEN
#           CALL fpkrdbout (funit, rndsup, addkey, cflag, vflag, 
#    &                      rtagny, sid, pktyp, begdate, enddate, irc) 

#        ELSE IF (datatyp .EQ. 'DC') THEN

#           CALL fdcrdbout (funit, rndsup, addkey, cflag, vflag,
#    *                      rtagny, sid, ddid, begdate, enddate, 
#    *                      loc_tz_cd, irc)

#        ELSE IF (datatyp .EQ. 'SV') THEN

#           CALL fsvrdbout (funit, rndsup, addkey, cflag, vflag,
#    *                      rtagny, sid, ddid, begdate, enddate, 
#    *                      loc_tz_cd, irc)

#        ELSE IF (datatyp .EQ. 'WL') Then

#           IF (hydra) wltyp = ' '
#           CALL fwlrdbout_hydra (funit, rndsup, rtagny, sid, begdtm,
#    *                            enddtm, loc_tz_cd, wltyp, irc)

#        ELSE IF (datatyp .EQ. 'QW') THEN

#           IF (hydra) THEN
#              qwparm = ' '
#              qwmeth = ' '
#           END IF
#           CALL fqwrdbout_hydra (funit, rndsup, rtagny, sid, begdtm,
#    *                            enddtm, loc_tz_cd, qwparm, qwmeth,
#    *                            irc)
#        END IF

#     END IF
fi

#     !  close files and exit
#997   CALL s_mclos
#     CALL s_sclose (funit, 'keep')
#     CALL nw_disconnect
#     GOTO 999

#     !  bad return (do a generic error message)
#998   irc = 3
#     CALL nw_error_handler (irc,'nwf_rdb_out','error',
#    *     'doing something','something bad happened')

#     !  Good return
#999   nwf_rdb_out = irc
#     RETURN
#     END
} # rdb_out
