# File - rdb_fill_beg_dtm.sh
#
# Purpose -
#
# Authors - Andy Halper <ashalper@usgs.gov> [Bourne Shell translation]
#           Scott Bartholoma <sbarthol@usgs.gov> [NW_RDB_FILL_END_DTM()]
#

rdb_fill_beg_dtm ()
{
    wyflag=$1
    begdat=$2
    begdtm=$3

    # convert date/times to 14 characters

    if [ $wyflag = true ]; then	# output = "<year-1>1001"

        if [ ${#begdat} -gt 4 ]; then # trim to just the year
            begdtm="${begdat:0:4}"
        else
            begdtm="$begdat"
	fi

        iyr=${begdtm:0:4}
        if [ $iyr -le 0 ]; then
            begdtm='00000000000000'
        else
            # year-1, month=Oct, day=01
	    begdtm="`expr $iyr - 1`1001"
	fi

	# Handle beginning of period - needs to be all zeros
        if [ "${begdtm:0:4}" = '0000' ]; then begdtm='00000000000000'; fi
    else			# regular year, not WY

        if [ ${#begdat} -gt 14 ]; then
            begdtm="${begdat:0:14}"
        else
            begdtm="$begdat"
	fi

    fi

    s_jstrlf begdtm 14

    begdtm=`echo $begdate | tr ' ' '0'`
    echo "$begdtm"
}
