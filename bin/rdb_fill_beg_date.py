# File - rdb_fill_beg_dtm.py
#
# Purpose - Python emulation of legacy NWIS, NW_RDB_FILL_BEG_DATE()
#           Fortran subroutine: takes an input date with < 8 chars and
#           fills it to 8 chars.
#
# Authors - Andrew Halper <ashalper@usgs.gov>
#           Scott Bartholoma <sbarthol@usgs.gov>
#
def rdb_fill_beg_date(
        wyflag,                 # flag if Water Year
        begdat                  # input date (may be < 8 chars)
):
    iyr = 0
    ipos = 0

    # convert date/times to 8 characters

    if wyflag == True:

        if len(begdat) > 4:
            begdate = begdat[0:3]
        else:
            begdate = begdat

        iyr = int(begdate)
        if iyr <= 0:
            begdate = "00000000"
        else:
            begdate = "{4}1001".format(iyr - 1)

    else:

        if len(begdat) > 8:
            begdate = begdat[0:7]
        else:
            begdate = begdat

    begdate = "{:<8}".format(begdate) # CALL S_JSTRLF (BEGDATE, 8)

    return begdate.replace(' ', '0')
