# File - rdb_fill_end_date.py
#
# Purpose - Python emulation of legacy NWIS, NW_RDB_FILL_END_DATE()
#           Fortran subroutine: takes an input date with < 8 chars and
#           fills it to 8 chars.
#
# Authors - Andrew Halper <ashalper@usgs.gov> [Python translation]
#           Scott Bartholoma <sbarthol@usgs.gov> [NW_RDB_FILL_END_DATE()]
def rdb_fill_end_date(
        wyflag,                 # flag if Water Year
        enddat                  # input date (may be < 8 chars)
):
    iyr = 0
    ipos = 0

    # **********************************************************************
    # * FORMATS
    # **********************************************************************

    # 1010  FORMAT (I4)
    # 2010  FORMAT (I4.4,2I2.2)

    # convert date/times to 8 characters

    if wyflag == True:     # output will be "<year>0930" for end of WY

        if len(enddat) > 4:
            enddate = enddat[0:3]
        else:
            enddate = enddat

        enddate = "{}0930".format(int(enddate))
        if enddate[0:3] == "9999":
            enddate = "99999999" # end of period is all nines

    else:                   # output will just be filled-out date

        if len(enddat) > 8:
            enddate = enddat[0:7]
        else:
            enddate = enddat

    enddate = "{:<8}".format(enddate)

    return enddate.replace(' ', '0')
