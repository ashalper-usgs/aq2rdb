# File - rdb_fill_end_dtm.sh
#
# Purpose -
#
# Authors - Andy Halper <ashalper@usgs.gov> [Bourne Shell translation]
#           Scott Bartholoma <sbarthol@usgs.gov> [NW_RDB_FILL_END_DTM()]
#

rdb_fill_end_dtm ()
{
    wyflag=$1                   # flag if Water Year
    enddat=$2                   # input date/time (may be < 14 chars)
    enddtm=$3                   # output date/time (filled to 14 chars)

    # convert date/times to 14 characters

    if [ "$wyflag" = true ]; then # output will be "<year>0930235959"

        if [ ${#enddat} -gt 4 ]; then
            enddtm="${enddat:0:4}"
        else
            enddtm="$enddat"
        fi

        if [ "${enddtm:0:4}" = '9999' ]; then
            enddtm='99999999999999' # end of period is all nines
        fi

    else                        # output will be filled-out date/time

        if [ ${#enddat} -gt 14 ]; then
            enddtm="${enddat:0:14}"
        else
            enddtm="$enddat"
        fi

    fi

    # emulate NWIS S_JSTRLF() Fortran subroutine
    enddtm=`echo "$enddtm" | awk '{ printf("%-14s", $1); }'`
    if [ "${enddtm:8:1}" = ' ' ]; then
        if [ "${enddtm:0:8}" = '99999999' ]; then
            enddtm="${enddtm:0:8}999999"
        else
            enddtm="${enddtm:0:8}235959"

        fi
    fi

    enddtm=`echo $enddate | tr ' ' '0'`
    echo "$enddtm"
}
