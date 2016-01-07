# File -- rdb_out.sh
#
# Purpose -- Top-level routine for outputting RDB format data
#
# Authors -- Andrew Halper <ashalper@usgs.gov> (Bourne Shell
#            translation of nwf_rdb_out.f)
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

        # check for completeness
        if [ "$rtagny" = ' ' -o "$sid" = ' ' -o
             \( "$datatyp" != 'DC' -a "$datatyp" != 'SV' -a
                "$datatyp" != 'WL' -a "$datatyp" != 'QW' -a
                "$stat" = ' ' \) -o \
             "$begdtm" = ' ' -o "$enddtm" = ' ' -o
             \( "$datatyp" != 'MS' -a "$datatyp" != 'PK' -a
           "$datatyp" != 'WL' -a "$datatyp" != 'QW' -a 
           "$ddid" = ' ' \)
          ]; then
            s_date "$cdate" "$ctime"
#           WRITE (0,2030) cdate, ctime, nline
#2030        FORMAT (A8, 1X, A6, 1X, 'Incomplete row (missing items)',
#    *              ' on line ', I5, '.')
            goto 9
        fi

        #  zero pad stat code IF type is DV
        if [ "$datatyp" = 'DV' ]; then
            s_jstrrt "$stat" 5
#           DO I = 1,5
#              IF (stat(i:i).EQ.' ') stat(i:i) = '0'
#           END DO
        fi

        if [ $first ]; then
            savetyp="$datatyp"  # save first data type
            mssav=${stat:0:1}
            first=false
            s_lgid                       # get user id and number
            s_ndget                      # get node data
            s_ggrp                       # get groups (for security)
            if [ ! $multiple ]; then
                # open output file
                if [ "$outpath" = ' ' ]; then
                    funit=6
                else
                    if [ ${#outpath} -gt 128 ]; then
                        rdb_cfil "$three" "$ctlfile" "$rtagny" "$sid" \
                            "$ddid" "$stat" "$bctdtm" "$ectdtm" "$nline"
                        irc=$?
                        goto 998
                    fi
                    rdbfile="$outpath"
                    s_file ' ' "$rdbfile" ' ' 'unknown' 'write' \
                        0 1 "$ipu" "$funit" "$irc" *7
#7
                    if [ $irc -ne 0 ]; then
                        rdb_cfil "$three" "$ctlfile" "$rtagny" "$sid" \
                           "$ddid" "$stat" "$bctdtm" "$ectdtm" "$nline"
                        irc=$?
                        goto 999
                    fi
                fi
            fi
        fi

        # if "multiple" not specified, all requests must
        # be the same data type as the first one

        if [ ! $multiple ]; then

            if [ "$datatyp" != "$savetyp" ]; then
                s_date "$cdate" "$ctime"
#              WRITE (0,2040) cdate, ctime, datatyp, savetyp, nline
#2040           FORMAT (A8, 1X, A6, 1X, 'Datatype of "', A,
#    *            '" not the same as the first request datatype of "',
#    *            A,'" on line ',I5,'.')
                goto 9
            fi

            if [ "$datatyp" = 'MS' -a ${stat:0:1} != "$mssav" ]; then
                # can't mix types of CSG measurements
                s_date "$cdate" "$ctime"
#              WRITE (0,2050) cdate,ctime,stat(1:1),mssav,nline
#2050           FORMAT (A8,1X,A6,1X,'Measurement type of "',A,
#    *              '" not compatible with the first ',
#    *              'measurement type of "',
#    *              A,'" on line ',I5,'.')
                goto 9
            fi
        fi

        # convert water years to date or datetime if -w specified
        rdb_fill_beg_dtm "$wyflag" "$bctdtm" "$begdtm"
        rdb_fill_end_dtm "$wyflag" "$ectdtm" "$enddtm"

        # if multiple, open a new output file - outpath is a prefix

        if [ "$multiple" ]; then
            # close previously open file
            if [ $funit -ge 0 -a $funit -ne 6 ]; then
                s_sclose $funit 'keep'
            fi
            # open a new file
            rdbfile="$outpath.$datatyp.$rtagny.$sid"
            rdblen=`expr ${#outpath} + 5 + ${#rtagny} + ${#sid}`
            if [ "$datatyp" != 'MS' -a "$datatyp" != 'PK' -a
                 "$datatyp" != 'WL' -a "$datatyp" != 'QW' ]; then
                lddid="$ddid"
                s_jstrlf "$lddid" 4
                rdbfile="$rdbfile.$lddid"
                rdblen=`expr $rdblen + 1 + ${#lddid}`
            fi
            if [ "$datatyp" != 'DC' -a "$datatyp" != 'SV' -a
                 "$datatyp" != 'WL' -a "$datatyp" != 'QW' ]; then
                rdbfile="$rdbfile.$stat"
                rdblen=`expr $rdblen + 1 + ${#stat}`
            fi
            rdbfile="$rdbfile.${begdtm:0:8}.rdb"
            rdblen=`expr $rdblen + 13`
            # TODO: translate "*8" from Fortran below
            s_file ' ' "$rdbfile" ' ' 'unknown' 'write' \
                0, 1, $ipu $funit $irc *8
#8
            if [ $irc -ne 0 ]; then
                s_date "$cdate" "$ctime"
#              WRITE (0,2060) cdate, ctime, irc, nline,
#    *                        rdbfile(1:rdblen)
#2060           FORMAT (A8, 1X, A6, 1X, 'Error ', I5,
#    *                 ' opening output file for line ',
#    *                 I5, '.', /, 16X, A)
                irc=rdb_cfil $three "$ctlfile" "$rtagny" "$sid" "$ddid" \
                    "$stat" "$bctdtm" "$ectdtm" $nline
                s_mclos
                goto 999
            fi
            s_date "$cdate" "$ctime"
#           WRITE (0,2070) cdate, ctime, rdbfile(1:rdblen)
#2070        FORMAT (A8, 1X, A6, 1X, 'Writing file ', A)
        fi

#        check DD for a P in column 1 - indicated parm code for PR DD search

        if [ ${ddid:0:1} = 'p' -o ${ddid:0:1} = 'P' ]; then
            parm=${ddid:1:5}
            s_jstrrt "$parm" 5
#           DO I = 1,5
#              IF (parm(i:i) .EQ. ' ') parm(i:i) = '0'
#           END DO
            get_prdd $rtdbnum "$rtagny" "$sid" "$parm" "$ddid" $irc
            if [ $irc -ne 0 ]; then
                s_date "$cdate" "$ctime"
#              WRITE (0,2035) cdate, ctime, rtagny, sid, parm, nline
#2035           FORMAT (A8, 1X, A6, 1X, 'No PRIMARY DD for station "', 
#    *                 A5, A15, '", parm "', A5, '" on line ', I5, '.')
                goto 9
            fi
        else
            # right justify DDID to 4 characters
            if [ datatyp .NE. 'MS' .AND. datatyp .NE. 'PK' .AND.
                    datatyp .NE. 'WL' .AND. datatyp .NE. 'QW' ]; then
              s_jstrrt "$ddid" 4
            fi
        fi

        # process the request
        if [ "$datatyp" = 'DV' ]; then

            fdvrdbout $funit false $rndsup "$addkey" "$vflag" \
                "$cflag" "$rtagny" "$sid" "$ddid" "$stat" \
                "$begdtm" "$enddtm" $irc

        elif [ "$datatyp" = 'UV' ]; then

            uvtyp=${stat:0:1}
            if [ "$uvtyp" != 'M' -a "$uvtyp" != 'N' -a "$uvtyp" != 'E'
                 -a "$uvtyp" != 'R' -a "$uvtyp" != 'S' -a 
                 "$uvtyp" != 'C' ]; then
                s_date "$cdate" "$ctime"
#              WRITE (0,2080) cdate, ctime, uvtyp, nline
#2080           FORMAT (A8, 1X, A6, 1X, 'Invalid unit-values type "', 
#    *                 A1, '" on line ', I5,'.')
            else
                if [ "$uvtyp" = 'M' ]; then inguvtyp='meas'; fi
                if [ "$uvtyp" = 'N' ]; then inguvtyp='msar'; fi
                if [ "$uvtyp" = 'E' ]; then inguvtyp='edit'; fi
                if [ "$uvtyp" = 'R' ]; then inguvtyp='corr'; fi
                if [ "$uvtyp" = 'S' ]; then inguvtyp='shift'; fi
                if [ "$uvtyp" = 'C' ]; then inguvtyp='da'; fi
                fuvrdbout $funit false $rtdbnum "$rndsup" "$cflag" \
                    "$vflag" "$addkey" "$rtagny" "$sid" "$ddid" \
                    "$inguvtyp" $sensor_type_id "$transport_cd" \
                    "$begdtm" "$enddtm" "$loc_tz_cd" $irc
            fi

        elif [ "$datatyp" = 'MS' ]; then

            mstyp=${stat:0:1}

            # Only standard meas types allowed when working from a control file
            # Pseudo-UV Types 1 through 3 are only good from the
            # command line or in hydra mode

            if [ "$mstyp" != 'C' -a "$mstyp" != 'M' -a
                 "$mstyp" != 'D' -a "$mstyp" != 'G' ]; then 
                s_date "$cdate" "$ctime"
#              WRITE (0,2090) cdate, ctime, mstyp, nline
#2090           FORMAT (A8, 1X, A6, 1X,
#    *                 'Invalid measurement file type "', A1,
#    *                 '" on line ', I5, '.')
            else

                fmsrdbout $funit $rtdbnum "$rndsup" "$addkey" "$cflag" \
                    "$vflag" "$rtagny" "$sid" "$mstyp" "$begdtm" \
                    "$enddtm" $irc

            fi

        elif [ "$datatyp" = 'PK' ]; then

            pktyp=${stat:0:1}
            if [ "$pktyp" != 'F' -a "$pktyp" != 'P' -a
                 "$pktyp" != 'B' ]; then
                s_date "$cdate" "$ctime"
#              WRITE (0,2100) cdate, ctime, pktyp, nline
#2100           FORMAT (A8,1X,A6,1X,'Invalid peak flow file type "',A1,
#    *              '" on line ',I5,'.')
            else

                fpkrdbout $funit "$rndsup" "$addkey" "$cflag" "$vflag" \
                    "$rtagny" "$sid" "$pktyp" "$begdtm" "$enddtm" $irc

            fi
#           
        elif [ "$datatyp" = 'DC' ]; then

            fdcrdbout $funit "$rndsup" "$addkey" "$cflag" "$vflag" \
                "$rtagny" "$sid" "$ddid" "$begdtm" "$enddtm" \
                "$loc_tz_cd" $irc

        elif [ "$datatyp" = 'SV' ]; then

            fsvrdbout $funit "$rndsup" "$addkey" "$cflag" "$vflag" \
                     "$rtagny" "$sid" "$ddid" "$begdtm" "$enddtm" \
                     "$loc_tz_cd" $irc
     
        fi

        # get next line from control file
#9        
        irc=rdb_cfil $two "$datatyp" "$rtagny" "$sid" "$ddid" "$stat" \
            "$bctdtm" "$ectdtm" $nline
        goto 6

    else                        # Not a control file

        sopt='10000000000000000000000000000000' # init control argument
        if [ ${#intyp} -gt 2 ]; then
            datatyp=${intyp:0:2}
        else
            datatyp="$intyp"
        fi

        # check data type
        datatyp=${$datatyp^^}

        if [ "$hydra" ]; then
            needstrt=true
            sopt="${sopt:0:7}1${sopt:8}"
            sopt="${sopt:0:11}2${sopt:12}"
            if [ "$datatyp" != 'DV' -a "$datatyp" != 'UV' -a
                 "$datatyp" != 'MS' -a "$datatyp" != 'WL' -a
                 "$datatyp" != 'QW']; then datatyp='UV'; fi

            # convert dates to 8 characters
            rdb_fill_beg_date "$wyflag" "$begdat" "$begdate"
            rdb_fill_end_date "$wyflag" "$enddat" "$enddate"

            # convert date/times to 14 characters
            rdb_fill_beg_dtm "$wyflag" "$begdat" "$begdtm"
            rdb_fill_end_dtm "$wyflag" "$enddat" "$enddtm"

        else

            if [ "$cflag" ]; then
                # Data type VT is pseudo-UV, no combining of date time
                # possible

                if [ "$datatyp" != 'DV' -a "$datatyp" != 'UV' -a
                     "$datatyp" != 'DC' -a "$datatyp" != 'SV' -a
                     "$datatyp" != 'MS' -a "$datatyp" != 'PK' -a 
                     "$datatyp" != 'WL' -a "$datatyp" != 'QW' ]; then

#11
                    datatyp=' '
                    echo 'Valid data types are:'
                    echo '   DV - Daily Values'
                    echo '   UV - Unit Values'
                    echo '   MS - Discharge Measurements'
                    echo '   PK - Peak Flows'
                    echo '   DC - Data Corrections'
                    echo '   SV - Variable Shift'
                    echo '   WL - Water Levels from GWSI'
                    echo '   QW - QW Data From QWDATA'
                    s_qryc 'Enter desired data type: ' ' ' 0 0 2 2 \
                        "$datatyp" *11 # <- TODO: translate Fortran
                    datatyp=${$datatyp^^}

                    if [ "$datatyp" != 'DV' -a "$datatyp" != 'UV' -a
                         "$datatyp" != 'DC' -a "$datatyp" != 'SV' -a
                         "$datatyp" != 'MS' -a "$datatyp" != 'PK' -a 
                         "$datatyp" != 'WL' -a "$datatyp" != 'QW' ]; then
                      s_bada 'Please answer ' \
                          '"DV", "UV", "MS", "PK", "DC", "ST",' \ 
                      ' "SV", "WL" or "QW".' *11 # <- TODO: translate Fortran
                    fi
                fi

            else

                if [ "$datatyp" != 'DV' -a "$datatyp" != 'UV' -a
                     "$datatyp" != 'DC' -a "$datatyp" != 'SV' -a
                     "$datatyp" != 'MS' -a "$datatyp" != 'VT' -a
                     "$datatyp" != 'PK' -a "$datatyp" != 'WL' -a
                     "$datatyp" != 'QW' ]; then

#12
                    datatyp=' '
                    echo 'Valid data types are:'
                    echo '   DV - Daily Values'
                    echo '   UV - Unit Values'
                    echo '   MS - Discharge Measurements'
                    echo '   VT - Site Visit Readings'
                    echo '   PK - Peak Flows'
                    echo '   DC - Data Corrections'
                    echo '   SV - Variable Shift'
                    echo '   WL - Water Levels from GWSI'
                    echo '   QW - QW Data From QWDATA'
                    s_qryc 'Enter desired data type: ' ' ' 0 0 2 2 \
                        "$datatyp" *12 # <- TODO: translate Fortran (GOTO?)
                    datatyp=${$datatyp^^}
                    if [ "$datatyp" != 'DV' -a "$datatyp" != 'UV' -a
                         "$datatyp" != 'DC' -a "$datatyp" != 'SV' -a
                         "$datatyp" != 'MS' -a "$datatyp" != 'VT' -a
                         "$datatyp" != 'PK' -a "$datatyp" != 'WL' -a
                         "$datatyp" != 'QW' ]; then
                        s_bada 'Please answer ' \
                            '"DV", "UV", "MS", "VT", "PK", "DC", "ST",' \ 
                        ' "SV", "WL" or "QW".' *12 # <- TODO: translate F77
                    fi
                fi

            fi

        fi

        # convert agency to 5 characters - default to USGS
        if [ "$inagny" = ' ' ]; then
            rtagny='USGS'
        else
            if [ ${#inagny} -gt 5 ]; then
                rtagny=${inagny:0:5}
            else
                rtagny="$inagny"
            fi
            s_jstrlf "$rtagny" 5
        fi

        # convert station to 15 characters
        if [ "$instnid" = ' ' ]; then
            needstrt=true
            if [ "$datatyp" = 'MS' -o "$datatyp" = 'PK' -o
                 "$datatyp" = 'WL' -o "$datatyp" = 'QW' ]; then
                sopt="${sopt:0:4}1${sopt:5}"
            else
                sopt="${sopt:0:4}2${sopt:5}"
            fi
        else
            if [ ${#instnid} -gt 15 ]; then
                sid=${instnid:0:15}
            else
                sid="$instnid"
            fi
            s_jstrlf "$sid" 15
        fi

#        DD is ignored for data types MS, PR, WL, and QW

        if [ "$datatyp" != 'MS' -a "$datatyp" != 'PK' -a
             "$datatyp" != 'WL' -a "$datatyp" != 'QW' ]; then

            # If type is VT, DDID is only needed IF parm and loc
            # number are not specified
            if [ \( datatyp != 'VT' -a "$inddid" = ' ' \) \
                 -o
                 \( datatyp = 'VT' -a "$inddid" = ' ' \
                    -a \( ${inddid:0:1} != 'P' -o "$inlocnu" = ' ' \) \) \
               ]; then
                needstrt=true
                sopt="${sopt:0:4}2${sopt:5}"
            else

                # If ddid starts with "P", it is a parameter code,
                # fill to 5 digits
                if [ ${inddid:0:1} = 'p' -o ${inddid:0:1} = 'P' ]; then
                    if [ ${#inddid} -gt 6 ]; then
                        parm=${inddid:1:5}
                    else
                        parm=${inddid:1}
                    fi
                    s_jstrrt "$parm" 5
#                 DO I = 1,5
#                    IF (parm(i:i) .EQ. ' ') parm(i:i) = '0'
#                 END DO
                else
                    parm=' '
                    # convert ddid to 4 characters
                    if [ ${#inddid} -gt 4 ]; then
                        ddid=${inddid:0:4}
                    else
                        ddid="$inddid"
                    fi
                    s_jstrrt ddid 4

                fi

            fi

        fi

        # further processing depends on data type

        if [ "$datatyp" = 'DV' ]; then # convert stat to 5 characters
            if [ "$instat" = ' ' ]; then
		needstrt=true
		sopt=${sopt:0:7}1${sopt:8}
            else
		if [ ${#instat} -gt 5 ]; then
                    stat=${instat:0:5}
		else
                    stat="$instat"
		fi
		s_jstrrt "$stat" 5
#              DO I = 1,5
#                 IF (stat(i:i) .EQ. ' ') stat(i:i) = '0'
#              END DO
	    fi
	fi

        if [ "$datatyp" = 'DV' -o "$datatyp" = 'DC' -o
	     "$datatyp" = 'SV' -o "$datatyp" = 'PK' ]; then

	    # convert dates to 8 characters
            if [ "$begdat" = ' ' -o
		 "$enddat" = ' ' ]; then
		needstrt=true
		if [ $wyflag ]; then
		    sopt=${sopt:0:8}4${sopt:9}
		else
		    sopt=${sopt:0:9}3${sopt:10}
		fi
            else
		rdb_fill_beg_date "$wyflag" "$begdat" "$begdate"
		rdb_fill_end_date "$wyflag" "$enddat" "$enddate"
	    fi

	fi

        if [ "$datatyp" = 'UV' ]; then

            if [ ! $hydra ]; then # get UV type
		uvtyp=${instat:0:1}
		if [ "$uvtyp" = 'm' ]; then uvtyp='M'; fi
		if [ "$uvtyp" = 'n' ]; then uvtyp='N'; fi
		if [ "$uvtyp" = 'e' ]; then uvtyp='E'; fi
		if [ "$uvtyp" = 'r' ]; then uvtyp='R'; fi
		if [ "$uvtyp" = 's' ]; then uvtyp='S'; fi
		if [ "$uvtyp" = 'c' ]; then uvtyp='C'; fi
		if [ "$uvtyp" != 'M' -a "$uvtyp" != 'N' -a 
		     "$uvtyp" != 'E' -a "$uvtyp" != 'R' -a 
		     "$uvtyp" != 'S' -a "$uvtyp" != 'C' ]; then
		    uvtyp_prompted=true
#50                
		    uvtyp=' '
		    s_qryc \
			'Unit values type (M, N, E, R, S, or C): ' \
			' ' 0 0 1 1 uvtyp *50 # <- TODO: translate F77
		    if [ "$uvtyp" = 'm' ]; then uvtyp='M'; fi
		    if [ "$uvtyp" = 'n']; then uvtyp='N'; fi
		    if [ "$uvtyp" = 'e']; then uvtyp='E'; fi
		    if [ "$uvtyp" = 'r']; then uvtyp='R'; fi
		    if [ "$uvtyp" = 's']; then uvtyp='S'; fi
		    if [ "$uvtyp" = 'c']; then uvtyp='C'; fi
		    if [ "$uvtyp" != 'M' -a "$uvtyp" != 'N' -a
			    "$uvtyp" != 'E' -a "$uvtyp" != 'R' -a
			    "$uvtyp" != 'S' -a "$uvtyp" != 'C' ]; then
			s_bada \
			    'Please answer "M", "N", "E", "R", "S", or "C".' \
			    *50		# <- TODO: translate F77
		    fi
		fi
	    fi

	    # convert date/times to 14 characters
	    if [ "$begdat" = ' ' -o
		 "$enddat" = ' ' ]; then
		needstrt=true
		if [ $wyflag ]; then
		    sopt=${sopt:0:8}4${sopt:9}
		else
		    sopt=${sopt:0:9}3${sopt:10}
		fi
            else
		rdb_fill_beg_dtm "$wyflag" "$begdat" "$begdtm"
		rdb_fill_end_dtm "$wyflag" "$enddat" "$enddtm"
	    fi

	fi

#        If hydra mode for UV data, set time zone code that in effect
#        for the first date for this station

        if [ $hydra -a "$datatyp" != 'UV' ]; then
            if [ ! eval key_get_zone_dst "$rtdbnum" "$rtagny" "$sid"
			"$tz_cd" "$local_time_fg" ]; then
		loc_tz_cd='UTC'	# default to UTC
            else
		if [ ! eval get_dflt_tzcd "$tz_cd" "local_time_fg"
			${begdtm:0:8} "$loc_tz_cd" ]; then
		    loc_tz_cd='UTC'       # default to UTC
		fi
            fi
	fi

#        if [ datatyp .EQ. 'MS') THEN       # get MS type
#           mstyp=instat(1:1)
#           CALL nwc_upcase (mstyp)

#           if [ mstyp .NE. 'C' .AND. mstyp .NE. 'M' .AND. 
#    *          mstyp .NE. 'D' .AND. mstyp .NE. 'G' .AND.
#    *          mstyp .NE. '1' .AND. mstyp .NE. '2' .AND. 
#    *          mstyp .NE. '3') THEN
#45             mstyp=' '

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
#              if [ mstyp .NE. 'C' .AND. mstyp .NE. 'M' .AND. 
#    *             mstyp .NE. 'D' .AND. mstyp .NE. 'G' .AND. 
#    *             mstyp .NE. '1' .AND. mstyp .NE. '2' .AND. 
#    *             mstyp .NE. '3')      CALL s_bada (
#    *                 'Please answer "C", "M", "G", or "1" to "3".',
#    *                 *45)
#           fi

#           if [ begdat(1:len(begdat)) .EQ. ' ' .OR.
#    *          enddat(1:len(enddat)) .EQ. ' ') THEN
#              needstrt=.true.
#              if [ wyflag) THEN
#                 sopt(9:9)='4'
#              ELSE
#                 sopt(10:10)='3'
#              fi

#           ELSE

#              if [ mstyp .GE. '1' .AND. mstyp .LE. '3') THEN
#                 #  doing pseudo-uv, convert date/times to 14 characters
#                 CALL nw_rdb_fill_beg_dtm (wyflag, begdat, begdtm)
#                 CALL nw_rdb_fill_end_dtm (wyflag, enddat, enddtm)
#              ELSE
#                 #  convert dates to 8 characters
#                 CALL nw_rdb_fill_beg_date (wyflag, begdat, begdate)
#                 CALL nw_rdb_fill_end_date (wyflag, enddat, enddate)
#              fi
#           fi
#        fi

#        if [ datatyp .EQ. 'VT') THEN     # get VT type
#           vttyp=instat(1:1)
#           CALL nwc_upcase (vttyp)

#           if [ vttyp .NE. 'P' .AND. vttyp .NE. 'R' .AND.
#    *          vttyp .NE. 'A' .AND. vttyp .NE. 'M' .AND.
#    *          vttyp .NE. 'F') THEN

#55             vttyp='A'

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
#              if [ vttyp .NE. 'P' .AND. vttyp .NE. 'R' .AND.
#    *             vttyp .NE. 'A' .AND. vttyp .NE. 'M' .AND.
#    *             vttyp .NE. 'F')    CALL s_bada (
#    *                     'Please answer "P", "R", "A", "M" or "F".',
#    *                     *55)

#           fi

#           #  See if we have the date range
#           if [ begdat(1:len(begdat)) .EQ. ' ' .OR.
#    *          enddat(1:len(enddat)) .EQ. ' ') THEN
#              needstrt=.TRUE.
#              if [ wyflag) THEN
#                 sopt(9:9) = '4'
#              ELSE
#                 sopt(10:10) = '3'
#              fi
#           fi

#           #  Doing pseudo-uv, convert date/times to 14 characters
#           CALL nw_rdb_fill_beg_dtm (wyflag, begdat, begdtm)
#           CALL nw_rdb_fill_end_dtm (wyflag, enddat, enddtm)

#        fi

#        if [ datatyp .EQ. 'PK') THEN       # get pk type

#           pktyp=instat(1:1)
#           if [ pktyp .EQ. 'f') pktyp = 'F'
#           if [ pktyp .EQ. 'p') pktyp = 'P'
#           if [ pktyp .EQ. 'b') pktyp = 'B'

#           if [ pktyp .NE. 'F' .AND. pktyp .NE. 'P' .AND. 
#    *          pktyp .NE. 'B') THEN
#46             pktyp=' '
#              CALL s_qryc ('Peak flow file retrieval type -' //
#    *              '|Full peaks only (F),' //
#    *              '|Partial peaks only (P),' //
#    *              '|Both Full and Partial peaks (B) - ' //
#    *              '|Please enter F, P, or B: ',' ',0,0,1,1,
#    *              pktyp, *46)
#              if [ pktyp .EQ. 'f') pktyp='F'
#              if [ pktyp .EQ. 'p') pktyp='P'
#              if [ pktyp .EQ. 'b') pktyp='B'
#              if [ pktyp .NE. 'F' .AND. pktyp .NE. 'P' .AND. 
#    *             pktyp .NE. 'B')    CALL s_bada (
#    *                     'Please answer "F", "P",  or "B".', *46)
#           fi

#        fi

#        if [ datatyp .EQ. 'WL') THEN
#          wltyp=instat(1:1)
#           if [ .NOT. (wltyp .GE. '1' .AND. wltyp .LE. '3')) wltyp=' '
#           #  convert date/times to 14 characters
#           if [ begdat(1:len(begdat)) .EQ. ' ' .OR.
#    *          enddat(1:len(enddat)) .EQ. ' ') THEN
#              needstrt=.true.
#              sopt(5:5)='1'
#              if [ wyflag) THEN
#                 sopt(9:9) = '4'
#              ELSE
#                 sopt(10:10) = '3'
#              fi
#           ELSE
#              CALL nw_rdb_fill_beg_dtm (wyflag, begdat, begdtm)
#              CALL nw_rdb_fill_end_dtm (wyflag, enddat, enddtm)
#           fi
#        fi

#        IF (datatyp .EQ. 'QW') THEN
#           qwparm=' '
#           IF (len(inddid) .GE. 2) THEN
#              qwparm=inddid(2:len(inddid))
#           fi
#           qwmeth=instat(1:len(instat))
#           #  convert date/times to 14 characters
#           IF (begdat(1:len(begdat)) .EQ. ' ' .OR.
#    *          enddat(1:len(enddat)) .EQ. ' ') THEN
#              needstrt=.true.
#              sopt(5:5) = '1'
#              IF (wyflag) THEN
#                 sopt(9:9) = '4'
#              ELSE
#                 sopt(10:10) = '3'
#              fi
#           ELSE
#              CALL nw_rdb_fill_beg_dtm (wyflag, begdat, begdtm)
#              CALL nw_rdb_fill_end_dtm (wyflag, enddat, enddtm)
#           fi
#        fi

#        IF (NEEDSTRT) THEN                      # call s_strt if needed
#           CALL S_MDUS (NW_OPRW, IRC, *998)     # get USER info 
#           IF (IRC .NE. 0) THEN
#              WRITE (0,2110)
#2110           FORMAT (/,'Unable to open ADAPS User file - Aborting.',/)
#              GO TO 998
#           fi
#           CALL S_LGID                          # get user info 
#           CALL S_MDUS (NW_READ,IRC,*998)
#           IF (IRC .EQ. 0) THEN                 # save the user info
#              DO I  =  1, 91
#                 HOLDBUFF(I)  =  USBUFF(I)
#              END DO

#              IF (SOPT(5:5) .EQ. '1' .OR. SOPT(5:5) .EQ. '2') THEN
#                 AGENCY = RTAGNY
#                 IF (INSTNID(1:LEN(INSTNID)) .NE. ' ') STNID = SID
#              fi
#              CALL S_MDUS (NW_UPDT, IRC, *998)  # save modified user info
#           fi

#           # call start routine
#           prgid='NWTS2RDB'
#           IF (titlline  .EQ. ' ') THEN
#              prgdes='TIME-SERIES TO RDB OUTPUT'
#           ELSE
#              IF (nwf_strlen(titlline) .GT. 80) THEN
#                 prgdes=titlline(1:80)
#              ELSE
#                 prgdes=titlline(1:nwf_strlen(titlline))
#              fi
#           fi
#           rdonly=1
#123         CALL s_strt (sopt, *998)
#           sopt(1:1) = '2'

#           IF (sopt(5:5) .EQ. '1' .OR. sopt(5:5) .EQ. '2') THEN
#              rtagny=agency              # get agency
#              sid=stnid                  # get stn ID
#              IF (sopt(5:5) .EQ. '2') THEN
#                 ddid=usddid             # and DD number
#              fi
#           fi

#           IF (ddid .EQ. ' ') THEN
#              IF (parm .NE. ' ' .AND. datatyp .NE. 'VT') THEN
#                 CALL nwf_get_prdd (rtdbnum, rtagny, sid, parm, ddid,
#    &                               irc)
#                 IF (irc .NE. 0) THEN
#                    WRITE (0,2120) rtagny, sid, parm
#                    GOTO 999
#                 fi
#              fi
#           fi

#           #  stat code
#           IF (sopt(8:8) .EQ. '1') stat=statcd

#           #  data type
#           IF (sopt(12:12) .EQ. '2') THEN
#              uvtyp_prompted=.TRUE.
#              IF (usdtyp .EQ. 'D') THEN
#                 datatyp='DV'
#                 cflag=.FALSE.
#              ELSE IF (usdtyp .EQ. 'V') THEN
#                 datatyp='DV'
#                 cflag=.TRUE.
#              ELSE IF (usdtyp .EQ. 'U') THEN
#                 datatyp='UV'
#                 uvtyp='M'
#              ELSE IF (usdtyp .EQ. 'N') THEN
#                 datatyp='UV'
#                 uvtyp='N'
#              ELSE IF (usdtyp .EQ. 'E') THEN
#                 datatyp='UV'
#                 uvtyp='E'
#              ELSE IF (usdtyp .EQ. 'R') THEN
#                 datatyp='UV'
#                 uvtyp='R'
#              ELSE IF (usdtyp .EQ. 'S') THEN
#                 datatyp='UV'
#                 uvtyp='S'
#              ELSE IF (usdtyp .EQ. 'C') THEN
#                 datatyp='UV'
#                 uvtyp='C'
#              ELSE IF (usdtyp .EQ. 'M') THEN
#                 datatyp='MS'
#              ELSE IF (usdtyp .EQ. 'X') THEN
#                 datatyp='VT'
#              ELSE IF (usdtyp .EQ. 'L') THEN
#                 datatyp='WL'
#              ELSE IF (usdtyp .EQ. 'Q') THEN
#                 datatyp='QW'
#              fi
#           fi

#           #  date range for water years
#           IF (sopt(9:9) .EQ. '4') THEN
#              IF (usyear .EQ. '9999') THEN
#                 begdtm='00000000000000'
#                 begdate='00000000'
#              ELSE
#                 READ (usyear,1010) iyr
#1010              FORMAT (I4)
#                 WRITE (usdate,2140) iyr-1,10,01
#2140              FORMAT (I4.4,2I2.2)
#                 begdtm=usdate // '000000'
#                 begdate=usdate
#              fi
#              IF (ueyear .EQ. '9999') THEN
#                 enddtm='99999999999999'
#                 enddate='99999999'
#              ELSE
#                 READ (ueyear,1010) iyr
#                 WRITE (uedate,2140) iyr,9,30
#                 enddtm=uedate // '235959'
#                 enddate=uedate
#              fi
#           fi

#           #  date range
#           IF (sopt(10:10) .EQ. '3') THEN
#                 begdate=usdate
#                 enddate=uedate
#                 begdtm=usdate // '000000'
#                 IF (uedate .EQ. '99999999') THEN
#                    enddtm='99999999999999'
#                 ELSE
#                    enddtm=uedate // '235959'
#                 fi
#           fi

#           #  Restore contents of user buffer
#           IF (IRC .EQ. 0) THEN
#              DO I  =  1, 91
#                 USBUFF(I)  =  HOLDBUFF(I)
#              END DO
#              CALL S_MDUS (NW_UPDT, IRC, *998)
#           ENDIF

#        ELSE

#           CALL s_lgid                 # get user id and number
#           CALL s_ndget                # get node data
#           CALL s_ggrp                 # get groups (for security)
#           CALL sen_dbop (rtdbnum)     # open Midas files
#           #  count program (counted by S_STRT above if needed)
#           IF (.NOT. nw_db_save_program_info ('NWTS2RDB')) THEN
#              CONTINUE       # ignore errors, we don't care if not counted
#           fi
#           #  get PRIMARY DD that goes with parm if parm supplied
#           IF (parm .NE. ' ' .AND. datatyp .NE. 'VT') THEN
#              CALL nwf_get_prdd (rtdbnum, rtagny, sid, parm, ddid, irc)
#              IF (irc .NE. 0) THEN
#                 WRITE (0,2120) rtagny, sid, parm
#2120              FORMAT (/,'No PRIMARY DD for station "',A5,A15,
#    *                 '", parm "',A5,'".  Aborting.',/)
#                 GOTO 999
#              fi
#           fi

#        fi

#        #  retrieving measured uvs and transport_cd not supplied, prompt for it
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
#           fi
#        fi

#        #  Open output file
#        IF (outpath .EQ. ' ') THEN
#           funit=6
#        ELSE
#           IF (len(outpath) .gt. 128) GOTO 998
#           rdbfile=outpath(1:len(outpath))
#           CALL s_file (' ', rdbfile, ' ', 'unknown', 'write', 0, 1,
#    *                  ipu, funit, irc, *90)
#90          IF (irc .NE. 0) THEN
#              WRITE (0,2130) rdbfile(1:nwf_strlen(rdbfile))
#2130           FORMAT (/,'Error ',I5,' opening output file:',/,3X,A,/)
#              GOTO 999
#           fi
#        fi

#        #  get data and output to files

#        IF (datatyp .EQ. 'DV') THEN

#           CALL fdvrdbout (funit, .false., rndsup, addkey, vflag,
#    &                      cflag, rtagny, sid, ddid, stat, 
#    *                      begdate, enddate, irc)

#        ELSE IF (datatyp .EQ. 'UV') THEN

#           IF (uvtyp .EQ. 'M') inguvtyp='meas'
#           IF (uvtyp .EQ. 'N') inguvtyp='msar'
#           IF (uvtyp .EQ. 'E') inguvtyp='edit'
#           IF (uvtyp .EQ. 'R') inguvtyp='corr'
#           IF (uvtyp .EQ. 'S') inguvtyp='shift'
#           IF (uvtyp .EQ. 'C') inguvtyp='da'

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
#           fi

#        ELSE IF (datatyp .EQ. 'VT') THEN

#           #  Get parm and loc number from DD IF not specified in arguments
#           IF (inddid(1:1) .NE. 'P' .OR. inlocnu .EQ. ' ') THEN
#              IF (.NOT. nw_db_key_get_dd_parm_loc (rtdbnum, rtagny, 
#    *                                              sid, ddid, parm,
#    *                                              loc_nu)) GOTO 997
#           ELSE
#              loc_nu=nwc_atoi(inlocnu)
#           fi

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

#           IF (hydra) wltyp=' '
#           CALL fwlrdbout_hydra (funit, rndsup, rtagny, sid, begdtm,
#    *                            enddtm, loc_tz_cd, wltyp, irc)

#        ELSE IF (datatyp .EQ. 'QW') THEN

#           IF (hydra) THEN
#              qwparm=' '
#              qwmeth=' '
#           fi
#           CALL fqwrdbout_hydra (funit, rndsup, rtagny, sid, begdtm,
#    *                            enddtm, loc_tz_cd, qwparm, qwmeth,
#    *                            irc)
#        fi

#     fi
fi

#     #  close files and exit
#997   CALL s_mclos
#     CALL s_sclose (funit, 'keep')
#     CALL nw_disconnect
#     GOTO 999

#     #  bad return (do a generic error message)
#998   irc=3
#     CALL nw_error_handler (irc,'nwf_rdb_out','error',
#    *     'doing something','something bad happened')

#     #  Good return
#999   nwf_rdb_out=irc
#     RETURN
#     END
} # rdb_out
