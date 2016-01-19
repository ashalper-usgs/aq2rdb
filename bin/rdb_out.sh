# File -- rdb_out.sh
#
# Purpose -- Top-level routine for outputting RDB format data
#
# Authors -- Andrew Halper <ashalper@usgs.gov> (Bourne Shell
#            translation of nwf_rdb_out.f)
#            Scott D. Bartholoma <sbarthol@usgs.gov>
#            Jim Cornwall <jcorn@usgs.gov>
#

rdb_out_goto ()
{
    case $1 in
        997)
            # close files and exit
            s_mclos
            s_sclose $funit 'keep'
            disconnect
            rdb_out_goto 999
            ;;
        998)
            # bad return (do a generic error message)
            irc=3
            error_handler $irc 'rdb_out' 'error' \
                'doing something' 'something bad happened'
            ;;
        999)
            # Good return
            nwf_rdb_out=$irc
            exit 0
            ;;
    esac
} # rdb_out_goto

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
        # TODO:
#        sensor_type_id = NW_NI4
    else
        transport_cd="$intrans"
        # TODO:
#        CALL s_upcase (transport_cd,1)
        sensor_type_id=0
    fi

    # TODO: this is the former database connection subroutine. Check
    # if aq2rdb service is alive here instead?

    #     IF (.NOT. nw_write_log_entry(1)) THEN
    #        CALL nw_write_error(6)
    #        irc = nw_get_error_number()
    #        GOTO 999
    #     END IF

    # set control file path
    if [ ${#ctlpath} -gt 128 ]; then
        rdb_out_goto 998
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
            "$stat" "$bctdtm" "$ectdtm" $nline
        irc=$?
#5
        if [ $irc -ne 0 ]; then
            rdb_out_goto 999
        fi
        s_date "$cdate" "$ctime"
        echo "$cdate $ctime Processing control file: $ctlfile"

        # get a line from the file
        first=true
        rdb_cfil "$two" "$datatyp" "$rtagny" "$sid" "$ddid" "$stat" \
            "$bctdtm" "$ectdtm" $nline
        irc=$?

        # weird control structure here is an artifact of
        # cautiously literal translation from Fortran 77
        until false; do
            if [ $irc -ne 0 ]; then
                rdb_cfil "$three" "$ctlfile" "$rtagny" "$sid" "$ddid" \
                    "$stat" "$bctdtm" "$ectdtm" $nline
                irc=$?

                # end of control file
                if [ "$first" != true ]; then
                    s_mclos         # close things down and exit cleanly
                    if [ $funit -ge 0 -a $funit -ne 6 ]; then
                        s_sclose $funit 'keep'
                    fi
                    irc=0
                fi
                rdb_out_goto 999
            fi

            # check data type
            if [ $hydra ]; then
                if [ "$datatyp" != 'DV' -a "$datatyp" != 'UV' -a
                     "$datatyp" != 'MS' -a "$datatyp" != 'WL' -a
                     "$datatyp" != 'QW' ]; then
                    s_date "$cdate" "$ctime"
                    echo "$cdate $ctime Invalid HYDRA data type" \
                        "\"$datatyp\" on line $nline."
                    goto 9
                fi
            else
                if [ "$datatyp" != 'DV' -a "$datatyp" != 'UV' -a
                        "$datatyp" != 'DC' -a "$datatyp" != 'SV' -a
                        "$datatyp" != 'MS' -a "$datatyp" != 'PK' ]; then
                    s_date "$cdate" "$ctime"
                    echo "$cdate $ctime Invalid data type \"$datatyp\" " \
                        "on line $nline."
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
                echo "$cdate $ctime Incomplete row (missing items) on " \
                    "line $nline."
                goto 9
            fi

            # zero pad stat code IF type is DV
            if [ "$datatyp" = 'DV' ]; then
                s_jstrrt "$stat" 5
                stat=`echo $stat | tr ' ' '0'`
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
                                "$ddid" "$stat" "$bctdtm" "$ectdtm" $nline
                            irc=$?
                            rdb_out_goto 998
                        fi
                        rdbfile="$outpath"
                        s_file ' ' "$rdbfile" ' ' 'unknown' 'write' \
                            0 1 "$ipu" "$funit" $irc *7
                        #7
                        if [ $irc -ne 0 ]; then
                            rdb_cfil "$three" "$ctlfile" "$rtagny" "$sid" \
                                "$ddid" "$stat" "$bctdtm" "$ectdtm" $nline
                            irc=$?
                            rdb_out_goto 999
                        fi
                    fi
                fi
            fi

            # if "multiple" not specified, all requests must
            # be the same data type as the first one

            if [ ! $multiple ]; then

                if [ "$datatyp" != "$savetyp" ]; then
                    s_date "$cdate" "$ctime"
                    echo "$cdate $ctime Datatype of \"$datatyp\" not " \
                        "the same as the first request datatype " \
                        "of \"$savetyp\" on line $nline."
                    goto 9
                fi

                if [ "$datatyp" = 'MS' -a ${stat:0:1} != "$mssav" ]; then
                    # can't mix types of CSG measurements
                    s_date "$cdate" "$ctime"
                    echo "$cdate $ctime Measurement type of " \
                        "\"${stat:0:1}\" not compatible with the " \
                        "first measurement type of \"$mssav\" on " \
                        "line $nline."
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
                    echo "$cdate $ctime Error $irc opening output file " \
                        "for line $nline."
                    echo "                $rdbfile"
                    irc=rdb_cfil $three "$ctlfile" "$rtagny" "$sid" "$ddid" \
                        "$stat" "$bctdtm" "$ectdtm" $nline
                    s_mclos
                    rdb_out_goto 999
                fi
                s_date "$cdate" "$ctime"
                echo "$cdate $ctime Writing file $rdbfile"
            fi

            # check DD for a P in column 1 - indicated parm code for PR DD
            # search

            if [ ${ddid:0:1} = 'p' -o ${ddid:0:1} = 'P' ]; then
                parm=${ddid:1:5}
                s_jstrrt "$parm" 5
                parm=`echo $parm | tr ' ' '0'`
                get_prdd $rtdbnum "$rtagny" "$sid" "$parm" "$ddid" $irc
                if [ $irc -ne 0 ]; then
                    s_date "$cdate" "$ctime"
                    echo "$cdate $ctime No PRIMARY DD for station " \
                        "\"$rtagny $sid\", parm \"$parm\" on line $nline."
                    goto 9
                fi
            else
                # right justify DDID to 4 characters
                if [ "$datatyp" != 'MS' -a "$datatyp" != 'PK' -a
                        "$datatyp" != 'WL' -a "$datatyp" != 'QW' ]; then
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
                    echo "$cdate $ctime Invalid unit-values type " \
                        "\"$uvtyp\" on line $nline."
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

                # Only standard meas types allowed when working from a
                # control file

                # Pseudo-UV Types 1 through 3 are only good from the
                # command line or in hydra mode

                if [ "$mstyp" != 'C' -a "$mstyp" != 'M' -a
                        "$mstyp" != 'D' -a "$mstyp" != 'G' ]; then 
                    s_date "$cdate" "$ctime"
                    echo "$cdate $ctime Invalid measurement file " \
                        "type \"$mstyp\" on line $nline."
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
                    echo "$cdate $ctime Invalid peak flow file type " \
                        "\"$pktyp\" on line $nline."
                else
                    fpkrdbout $funit "$rndsup" "$addkey" "$cflag" "$vflag" \
                        "$rtagny" "$sid" "$pktyp" "$begdtm" "$enddtm" $irc
                fi

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
            irc=rdb_cfil $two "$datatyp" "$rtagny" "$sid" "$ddid" "$stat" \
                "$bctdtm" "$ectdtm" $nline

        done

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

        # DD is ignored for data types MS, PR, WL, and QW

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
                    parm=`echo $parm | tr ' ' '0'`
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
                stat=`echo $stat | tr ' ' '0'`
            fi
        fi

        if [ "$datatyp" = 'DV' -o "$datatyp" = 'DC' -o
             "$datatyp" = 'SV' -o "$datatyp" = 'PK' ]; then

            # convert dates to 8 characters
            if [ "$begdat" = ' ' -o "$enddat" = ' ' ]; then
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
                            *50         # <- TODO: translate F77
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

        # If Hydra mode for UV data, set time zone code that in effect
        # for the first date for this station

        if [ $hydra -a "$datatyp" != 'UV' ]; then
            if [ ! eval key_get_zone_dst "$rtdbnum" "$rtagny" "$sid"
                        "$tz_cd" "$local_time_fg" ]; then
                loc_tz_cd='UTC' # default to UTC
            else
                if [ ! eval get_dflt_tzcd "$tz_cd" "local_time_fg"
                     ${begdtm:0:8} "$loc_tz_cd" ]; then
                    loc_tz_cd='UTC'       # default to UTC
                fi
            fi
        fi

        if [ "$datatyp" = 'MS' ]; then # get MS type
            mstyp=${instat:0:1}
            mstyp=${$mstyp^^}

            if [ "$mstyp" != 'C' -a "$mstyp" != 'M' -a 
                 "$mstyp" != 'D' -a "$mstyp" != 'G' -a
                 "$mstyp" != '1' -a "$mstyp" != '2' -a 
                 "$mstyp" != '3' ]; then
                mstyp=' '
            fi
#45
            echo 'Measurement file retrieval type -'
            echo '  C - Crest Stage Gage measurements,'
            echo '  M - Discharge Measurements,'
            echo '  D - Detailed Discharge Measurements,'
            echo '  G - Gage Inspections,'
            echo '  1 - Pseudo UV, measurement discharge,'
            echo '  2 - Pseudo UV, measurement stage, or'
            echo '  3 - Pseudo UV, mean index velocity'

            s_qryc '|Enter C, M, D, G, or 1 to 3: ' \
                ' ' 0 0 1 1 "$mstyp" *45 # <- TODO translate F77 GOTO?
            mstyp=${$mstyp^^}
            if [ "$mstyp" != 'C' -a "$mstyp" != 'M' -a 
                 "$mstyp" != 'D' -a "$mstyp" != 'G' -a 
                 "$mstyp" != '1' -a "$mstyp" != '2' -a 
                 "$mstyp" != '3' ]; then s_bada \
                    'Please answer "C", "M", "G", or "1" to "3".' \
                    *45; fi
        fi

        if [ "$begdat" = ' ' -o "$enddat" = ' ' ]; then
            needstrt=true
            if [ $wyflag ]; then
                sopt="${sopt:0:8}4${sopt:9}"
            else
                sopt="${sopt:0:9}3${sopt:10}"
            fi
        else

            if [ "$mstyp" >= '1' -a "$mstyp" <= '3' ]; then
                # doing pseudo-uv, convert date/times to 14 characters
                rdb_fill_beg_dtm "$wyflag" "$begdat" "$begdtm"
                rdb_fill_end_dtm "$wyflag" "$enddat" "$enddtm"
              else
                # convert dates to 8 characters
                rdb_fill_beg_date "$wyflag" "$begdat" "$begdate"
                rdb_fill_end_date "$wyflag" "$enddat" "$enddate"
            fi
        fi
    fi

    if [ "$datatyp" = 'VT' ]; then # get VT type
        vttyp="${instat:0:1}"
        vttyp=${$vttyp^^}

        if [ "$vttyp" != 'P' -a "$vttyp" != 'R' -a
             "$vttyp" != 'A' -a "$vttyp" != 'M' -a
             "$vttyp" != 'F' ]; then

            vttyp='A'

            echo 'SiteVisit Pseudo UV readings retrieval type -'
            echo '  P - Retrieve sensor insp. primary reference readings'
            echo '  R - Retrieve sensor insp. primary recorder readings'
            echo '  A - Retrieve sensor insp. all readings'
            echo '  M - Retrieve QW monitor readings'
            echo '  F - Retrieve QW field meter readings'

            s_qryc '|Enter P, R, A, M, or F (<CR> = A):' \
                ' ' 0 1 1 1 "$vttyp" *55 # <- TODO: translate F77
            vttyp=${$vttyp^^}
            if [ "$vttyp" != 'P' -a "$vttyp" != 'R' -a
                    "$vttyp" != 'A' -a "$vttyp" != 'M' -a
                    "$vttyp" != 'F' ]; then
                s_bada \
                    'Please answer "P", "R", "A", "M" or "F".' \
                    *55         # <- TODO: translate F77
            fi

        fi

        # See if we have the date range
        if [ "$begdat" = ' ' -o "$enddat" = ' ' ]; then
            needstrt=true
            if [ $wyflag ]; then
                sopt="${sopt:0:8}4${sopt:9}"
            else
                sopt="${sopt:0:9}3${sopt:10}"
            fi
        fi

        # Doing pseudo-uv, convert date/times to 14 characters
        rdb_fill_beg_dtm "$wyflag" "$begdat" "$begdtm"
        rdb_fill_end_dtm "$wyflag" "$enddat" "$enddtm"

    fi

    if [ "$datatyp" = 'PK' ]; then # get PK type

        pktyp="${instat:0:1}"
        if [ "$pktyp" = 'f' ]; then pktyp='F'; fi
        if [ "$pktyp" = 'p' ]; then pktyp='P'; fi
        if [ "$pktyp" = 'b' ]; then pktyp='B'; fi

        if [ "$pktyp" != 'F' -a "$pktyp" != 'P' -a "$pktyp" != 'B' ]; then
#46
            pktyp=' '
            s_qryc 'Peak flow file retrieval type -'\
            '|Full peaks only (F),'\
            '|Partial peaks only (P),'\
            '|Both Full and Partial peaks (B) - '\
            '|Please enter F, P, or B: ' ' ' 0 0 1 1\
            "$pktyp" *46 # <- TODO: translate F77 loop
            if [ "$pktyp" = 'f' ]; then pktyp='F'; fi
            if [ "$pktyp" = 'p' ]; then pktyp='P'; fi
            if [ "$pktyp" = 'b' ]; then pktyp='B'; fi
            if [ "$pktyp" != 'F' -a "$pktyp" != 'P' -a "$pktyp" != 'B' ]; then
                s_bada \
                    'Please answer "F", "P",  or "B".' *46 # <- TODO: tranlate
            fi

    fi

        if [ "$datatyp" = 'WL' ]; then
            wltyp="${instat:0:1}"
            if [ ! \( "$wltyp" >= '1' -a "$wltyp" <= '3' \) ]; then
                wltyp=' '
            fi
            # convert date/times to 14 characters
            if [ "$begdat" = ' ' -o "$enddat" = ' ' ]; then
                needstrt=true
                sopt="${sopt:0:4}1${sopt:5}"
                if [ $wyflag ]; then
                    sopt="${sopt:0:8}4${sopt:9}"
                else
                    sopt="${sopt:0:9}3${sopt:10}"
                fi
            else
                rdb_fill_beg_dtm "$wyflag" "$begdat" "$begdtm"
                rdb_fill_end_dtm "$wyflag" "$enddat" "$enddtm"
            fi
        fi

        if [ "$datatyp" = 'QW' ]; then
            qwparm=' '
           if [ ${#inddid} -ge 2 ]; then
               qwparm="${inddid:1}"
           fi
           qwmeth="$instat"
           # convert date/times to 14 characters
           if [ "$begdat" = ' ' -o "$enddat" = ' ' ]; then
               needstrt=true
               sopt="${sopt:0:4}1${sopt:5}"
               if [ $wyflag ]; then
                   sopt="${sopt:0:8}4${sopt:9}"
               else
                   sopt="${sopt:0:9}3${sopt:10}"
               fi
           else
               rdb_fill_beg_dtm "$wyflag" "$begdat" "$begdtm"
               rdb_fill_end_dtm "$wyflag" "$enddat" "$enddtm"
           fi
        fi

        if [ $needstrt ]; then  # call s_strt if needed
            # TODO: translate F77
            s_mdus nw_oprw, irc, *998 # get USER info 
            if [ $irc -ne 0 ]; then
                echo 'Unable to open ADAPS User file - Aborting.'
              rdb_out_goto 998          # <- TODO
            fi
            s_lgid          # get user info 
            s_mdus nw_read,irc *998 # <- TODO: translate F77
            if [ $irc -eq 0 ]; then # save the user info
                holdbuff="$usbuff"
                if [ "${sopt:4:1}" = '1' -o "${sopt:4:1}" = '2' ]; then
                    agency="$rtagny"
                    if [ "$instnid" != ' ' ]; then stnid="$sid"; fi
                fi
                # TODO: translate F77
                s_mdus nw_updt irc *998  # save modified user info
            fi

            # call start routine
            prgid='aq2rdb'
            if [ "$titlline" = ' ' ]; then
                prgdes='TIME-SERIES TO RDB OUTPUT'
            else
                if [ ${#titlline} -gt 80 ]; then
                    prgdes="${titlline:0:80}"
                else
                    prgdes="$titlline"
              fi
            fi
            rdonly=1
#123
            s_strt "$sopt" *998 # <- TODO: translate F77
            sopt="2${sopt:1}"

            if [ "${sopt:4:1}" = '1' -o "${sopt:4:1}" = '2' ]; then
                rtagny="$agency" # get agency
                sid="$stnid"     # get stn ID
                if [ "${sopt:4:1}" = '2' ]; then
                    ddid="$usddid" # and DD number
                fi
            fi

           if [ "$ddid" = ' ' ]; then
               if [ "$parm" != ' ' -a "$datatyp" != 'VT' ]; then
                 get_prdd "$rtdbnum" "$rtagny" "$sid" "$parm" ddid irc
                 if [ $irc -ne 0 ]; then
                     echo "$rtagny$sid$parm"
                     rdb_out_goto 999   # <- TODO
                 fi
               fi
           fi

           # stat code
           if [ "${sopt:7:1}" = '1' ]; then stat="$statcd"; fi

           # data type
           if [ "${sopt:11:1}" = '2' ]; then
               uvtyp_prompted=true
               if [ usdtyp = 'D' ]; then
                   datatyp='DV'
                   cflag=false
               elif [ "$usdtyp" = 'V' ]; then
                   datatyp='DV'
                   cflag=true
               elif [ "$usdtyp" = 'U' ]; then
                   datatyp='UV'
                   uvtyp='M'
               elif [ "$usdtyp" = 'N' ]; then
                   datatyp='UV'
                   uvtyp='N'
               elif [ "$usdtyp" = 'E' ]; then
                   datatyp='UV'
                   uvtyp='E'
               elif [ "$usdtyp" = 'R' ]; then
                   datatyp='UV'
                   uvtyp='R'
               elif [ "$usdtyp" = 'S' ]; then
                   datatyp='UV'
                   uvtyp='S'
               elif [ "$usdtyp" = 'C' ]; then
                   datatyp='UV'
                   uvtyp='C'
               elif [ "$usdtyp" = 'M' ]; then
                   datatyp='MS'
               elif [ "$usdtyp" = 'X' ]; then
                   datatyp='VT'
               elif [ "$usdtyp" = 'L' ]; then
                   datatyp='WL'
               elif [ "$usdtyp" = 'Q' ]; then
                   datatyp='QW'
               fi
           fi

           # date range for water years
           if [ "${sopt:8:1}" = '4' ]; then
               if [ "$usyear" = '9999' ]; then
                   begdtm='00000000000000'
                   begdate='00000000'
               else
                   # TODO:
#                 READ (usyear,1010) iyr
#1010              FORMAT (I4)
#                 WRITE (usdate,2140) iyr-1,10,01
#2140              FORMAT (I4.4,2I2.2)
                   begdtm="$usdate"000000
                   begdate="$usdate"
               fi
               if [ "$ueyear" = '9999' ]; then
                   enddtm='99999999999999'
                   enddate='99999999'
               else
#                 READ (ueyear,1010) iyr
#                 WRITE (uedate,2140) iyr,9,30
                   enddtm="$uedate"235959
                   enddate="$uedate"
               fi
           fi

           # date range
           if [ "${sopt:9:1}" = '3' ]; then
               begdate="$usdate"
               enddate="$uedate"
               begdtm="$usdate"000000
               if [ "$uedate" = '99999999' ]; then
                   enddtm='99999999999999'
               else
                   enddtm="$uedate"235959
               fi
           fi

           # Restore contents of user buffer
           if [ $irc -eq 0 ]; then
               usbuff="$holdbuff"
              s_mdus nw_updt $irc *998 # <- TODO
           fi

        else

            s_lgid                # get user id and number
            s_ndget               # get node data
            s_ggrp                # get groups (for security)
            sen_dbop "$rtdbnum" # open Midas files
            # count program (counted by S_STRT above if needed)
            db_save_program_info 'aq2rdb'
            # get PRIMARY DD that goes with parm if parm supplied
            if [ "$parm" != ' ' -a "$datatyp" != 'VT' ]; then
                get_prdd "$rtdbnum" "$rtagny" "$sid" "$parm" "$ddid" $irc
                if [ $irc -ne 0 ]; then
                    echo
                    # TODO: $rtagny needs to be output in a 5 char
                    # field here
                    echo "No PRIMARY DD for station " \
                        "\"$rtagny $sid\", parm \"$parm\". Aborting."
                    echo
                 rdb_out_goto 999
                fi
            fi

        fi

        # retrieving measured UVs and transport_cd not supplied,
        # prompt for it
        if [ $uvtyp_prompted -a "$datatyp" = 'UV' -a
             \( "$uvtyp" = 'M' -o "$uvtyp" = 'N' \) -a
             "$transport_cd" = ' ' ]; then
            query_meas_uv_type "$rtagny" "$sid" "$ddid" "$begdtm" \
                "$enddtm" "$loc_tz_cd" "$transport_cd" \
                "$sensor_type_id" *998 # <- TODO translate F77
            if [ "$transport_cd" = ' ' ]; then
                echo "No MEASURED UV data for station \"$rtagny$sid\"" \
                    ", DD \"$ddid\". Aborting."
                rdb_out_goto 999        # <- TODO
            fi
        fi

        # Open output file
        if [ "$outpath" = ' ' ]; then
            funit=6
        else
            if [ ${#outpath} -gt 128 ]; then rdb_out_goto 998; fi
            rdbfile="$outpath"
            s_file ' ' "$rdbfile" ' ' 'unknown' 'write' 0 1 \
                $ipu $funit $irc *90 # <- TODO translate F77
#90
            if [ $irc -ne 0 ]; then
                echo "Error opening output file:"
                echo "   $rdbfile"
                rdb_out_goto 999
            fi
        fi

        # get data and output to files

        if [ "$datatyp" = 'DV' ]; then

            fdvrdbout $funit false $rndsup $addkey $vflag \
                $cflag "$rtagny" "$sid" "$ddid" "$stat" \
                "$begdate" "$enddate" $irc

        elif [ "$datatyp" = 'UV' ]; then

            if [ uvtyp = 'M' ]; then inguvtyp='meas'; fi
            if [ uvtyp = 'N' ]; then inguvtyp='msar'; fi
            if [ uvtyp = 'E' ]; then inguvtyp='edit'; fi
            if [ uvtyp = 'R' ]; then inguvtyp='corr'; fi
            if [ uvtyp = 'S' ]; then inguvtyp='shift'; fi
            if [ uvtyp = 'C' ]; then inguvtyp='da'; fi

            fuvrdbout $funit false "$rtdbnum" $rndsup $cflag \
                $vflag $addkey "$rtagny" "$sid" "$ddid" "$inguvtyp" \
                "$sensor_type_id" "$transport_cd" "$begdtm" \
                "$enddtm" "$loc_tz_cd" $irc

        elif [ "$datatyp" = 'MS' ]; then

            if [ $hydra -o \( "$mstyp" >= '1' -a "$mstyp" <= '3' \) ]; then
                if [ $hydra ]; then mstyp=' '; fi
                fmsrdbout_hydra $funit $rndsup "$rtagny" "$sid" \
                    "$begdtm" "$enddtm" "$loc_tz_cd" \
                    "$mstyp" $irc
            else
                fmsrdbout $funit "$rtdbnum" $rndsup $addkey $cflag \
                    $vflag "$rtagny" "$sid" "$mstyp" "$begdate" \
                    "$enddate" $irc
            fi

        elif [ "$datatyp" = 'VT' ]; then

            # Get parm and loc number from DD IF not specified in
            # arguments
            if [ "${inddid:0:1}" != 'P' -o "$inlocnu" = ' ' ]; then
                if [ ! db_key_get_dd_parm_loc "$rtdbnum" "$rtagny" 
                                             "$sid" "$ddid" "$parm"
                                             "$loc_nu" ]; then goto 997; fi
            else
              loc_nu=nwc_atoi "$inlocnu"
            fi

            fvtrdbout_hydra $funit $rndsup "$rtagny" "$sid" "$parm" \
                "$loc_nu" "$begdtm" "$enddtm" "$loc_tz_cd" \
                "$vttyp" $irc

        elif [ "$datatyp" = 'PK' ]; then
            fpkrdbout $funit $rndsup $addkey $cflag $vflag \
                "$rtagny" "$sid" "$pktyp" "$begdate" "$enddate" $irc

        elif [ "$datatyp" = 'DC' ]; then

            fdcrdbout $funit $rndsup $addkey $cflag $vflag \
                "$rtagny" "$sid" "$ddid" "$begdate" "$enddate" \
                "$loc_tz_cd" $irc

        elif [ "$datatyp" = 'SV' ]; then

            fsvrdbout $funit $rndsup $addkey $cflag $vflag \
                "$rtagny" "$sid" "$ddid" "$begdate" "$enddate" \
                "$loc_tz_cd" $irc

        elif [ "$datatyp" = 'WL' ]; then

            if [ $hydra ]; then wltyp=' '; fi
            fwlrdbout_hydra $funit $rndsup "$rtagny" "$sid" "$begdtm" \
                "$enddtm" "$loc_tz_cd" "$wltyp" $irc

        elif [ "$datatyp" = 'QW' ]; then

            if [ $hydra ]; then
                qwparm=' '
                qwmeth=' '
            fi
            fqwrdbout_hydra $funit $rndsup "$rtagny" "$sid" "$begdtm" \
                "$enddtm" "$loc_tz_cd" "$qwparm" "$qwmeth" \
                $irc
        fi
    fi

} # rdb_out
