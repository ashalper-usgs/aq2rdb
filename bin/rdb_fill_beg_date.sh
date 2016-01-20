# File - rdb_fill_beg_date.sh
#
# Purpose -
#
# Authors - Andy Halper <ashalper@usgs.gov>
#           Translation of NW_RDB_FILL_BEG_DATE().
#
#           Scott Bartholoma <sbarthol@usgs.gov>
#           NW_RDB_FILL_BEG_DATE().
#

rdb_fill_beg_date ()
{
    wyflag="$1"; shift;		# flag if Water Year
    begdat="$1";		# input date (may be < 8 chars)
    # TODO:    begdate;		# output date (filled to 8 chars)

    #     convert date/times to 8 characters

    if [ $wyflag = true ]; then

        if [ ${#begdat} -gt 4 ]; then
            begdate="${begdat:0:4}"
        else
            begdate="$begdat"
	fi
        iyr="${begdate:0:4}"
        if [ $iyr -le 0 ]; then
            begdate='00000000'
        else
	    begdate="`expr $iyr - 1`1001"
	fi
    else

        if [ ${#begdat} -gt 8 ]; then
            begdate="${begdat:0:8}"
        else
            begdate="$begdat"
        fi

    fi

    s_jstrlf "$begdate" 8

    begdate=`echo $begdate | tr ' ' '0'`
    return
}
