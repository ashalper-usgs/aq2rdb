# File - rdb_fill_end_dtm.py
#
# Purpose - Python emulation of legacy NWIS, NW_RDB_FILL_END_DTM():
#           convert date/times to 14 characters.
#
# Authors - Andy Halper <ashalper@usgs.gov> [Python translation]
#           Scott Bartholoma <sbarthol@usgs.gov> [NW_RDB_FILL_END_DTM()]
#

def rdb_fill_end_dtm(
        wyflag,                 # flag if Water Year
        enddat                  # input date/time (may be < 14 chars)
):

    if wyflag == True:          # output will be "<year>0930235959"

        if len(enddat) > 4:
            enddtm = enddat[0:3]
        else:
            enddtm = enddat

        if enddtm[0:3] == "9999":
            enddtm = "99999999999999" # end of period is all nines

    else:                       # output will be filled-out date/time

        if len(enddat) > 14:
            enddtm = enddat[0:13]
        else:
            enddtm = enddat

    # emulate NWIS S_JSTRLF() Fortran subroutine
    enddtm = "{:<14}".format(enddtm)
    
    if enddtm[7] == ' ':
        if enddtm[0:7] == "99999999":
            # TODO: this is odd; probably should re-check Fortran source
            enddtm = enddtm[0:7] + "999999"
        else:
            enddtm = enddtm[0:7] + "235959"

    # TODO: possibly incorrect variable reference here; re-check
    # Fortran source
    enddtm = enddat.replace(' ', '0')
    return enddtm
