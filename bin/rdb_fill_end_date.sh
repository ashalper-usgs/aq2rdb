# File - rdb_fill_end_date.sh
#
# Purpose - Takes an input date with < 8 chars and fills it to 8 chars.
#
# Authors - Andy Halper <ashalper@usgs.gov>
#           Translation of NW_RDB_FILL_END_DATE().
#
#           Scott Bartholoma <sbarthol@usgs.gov>
#           NW_RDB_FILL_END_DATE().
#

rdb_fill_end_date ()
{
    wyflag=$1;
    enddat=$2;
    enddate=$3;

    # convert date/times to 8 characters

    if [ "$wyflag" = true ]; then
        # output will be "<year>0930" for end of WY

        if [ ${#enddat} -gt 4 ]; then
            enddate=${enddat:0:4}
        else
            enddate="$enddat"
        fi

        if [ ${enddate:0:4} = '9999' ]; then
            # end of period is all nines
            enddate='99999999'
        fi

    else                        # output will just be filled-out date

        if [ ${#enddat} -gt 8 ]; then
            enddate=${enddat:0:8}
        else
            enddate="$enddat"
        fi

    fi

    enddate=`echo "$enddate" | awk '{ printf("%-8s", $1); }'`

    enddate=`echo $enddate | tr ' ' '0'`
    echo "$enddate"
}
