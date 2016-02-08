# File - rdb_fill_beg_dtm.py
#
# Purpose - Python emulation of legacy NWIS, NW_RDB_FILL_END_DTM():
#           convert date/times to 14 characters.
#
# Authors - Andy Halper <ashalper@usgs.gov> [Python translation]
#           Scott Bartholoma <sbarthol@usgs.gov> [NW_RDB_FILL_END_DTM()]
#

def rdb_fill_beg_dtm(wyflag, begdat):

    if wyflag == True: # output = "<year-1>1001"

        if len(begdat) > 4:     # trim to just the year
            begdtm = begdat[0:3]
        else:
            begdtm = begdat

        iyr = int(begdtm[0:3])
        if iyr <= 0:
            begdtm = "00000000000000"
        else:
            # year - 1, month = Oct, day = 01
            begdtm = str(iyr - 1) + "1001"

        # Handle beginning of period - needs to be all zeros
        if begdtm[0:3] == "0000": begdtm = "00000000000000"
    else:                       # regular year, not WY

        if len(begdat) > 14:
            begdtm = begdat[0:13]
        else:
            begdtm = begdat

    begdtm = "{:<14}".format(begdtm).replace(' ', '0')
    return begdtm
