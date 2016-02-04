# File - rdb_cfil.py
#
# Purpose - Python emulation of legacy NWIS ADAPS, nwc_rdb_cfil() C
#           function: "This module is used to read information from a
#           control file".
#
# Authors - Andrew Halper <ashalper@usgs.gov> [Python translation]
#           Scott D. Bartholoma <sbarthol@usgs.gov> [nwc_rdb_cfil()]
#

import sys

maxplin = 256

# @return (int) Returns 0 if all OK, else error code
def rdb_cfil(
        iop,                    # command option
        datatyp,                # data type
        rtagny,                 # agency code
        sid,                    # station number
        ddid,                   # DD number
        stat,                   # statistic code
        begdtm,                 # begin date/time
        enddtm,                 # end date/time
        nline                   # number of lines of data
):
    # Local Variable Declarations
    infile = None
    rdbfile = None
    cp = None
    irc = 0
    cols = ["DATATYPE", "AGENCY", "STATION", "DDID",
            "SUBTYPE", "BEGDATE", "ENDDATE"]
    havecol = None
    colpos = None
    namline = None
    colnam = None
    defline = None
    coldef = None
    numcol = 0
    line = None
    colval = None
    eof = 0
    n = 0
    i = 0

    if iop == 1:
        # open the file - name in datatyp
        rdbfile = fstr2cstr(datatyp, dlen)
        try:
            infile = open(rdbfile, "r")
        except IOError as e:
            sys.stderr.write(
                "nwrdbin - Error %d opening RDB control file \"%s\".\n",
                e.errno, rdbfile
            )
            return e.errno

        # get and check RDB column definitions
        eof = True

        # initialize
        for i in range(0, 7):
            havecol[i] = False
        nline = 0

        # find the first non-comment line
        for namline in infile.readline(maxplin):
            nline += 1
            if namline[0] != '#':
                # read the column headings
                numcol = 0
                colnam[numcol] = rdb_tabtok(namline)
                while colnam[numcol] != NULL:
                    # see if it is one of the columns we are expecting
                    for i in range(0, 7):
                        if colnam[numcol] == cols[i]:
                            havecol[i] = True
                            colpos[i] = numcol
                            break
	
                    # get the next token
                    colnam[++numcol] = rdb_tabtok(NULL)
	
                # see if we got what we expected
                for i in range(0, 7):
                    if havecol[i] != True:
                        sys.stderr.write(
                            "RDB control file column definitions on " +
                            "line %d did not have all the expected " +
                            "columns (rdb_cfil)\n", nline
                        )
                        return -1

	eof = False

        if eof == True:
            return 1

        # find the next non-comment line
        eof = True
        for namline in infile.readline(maxplin):
            nline += 1
            if defline[0] != '#':
                # read the column definitions
                n = 0
                coldef[n] = rdb_tabtok(defline)
                while coldef[n] != NULL:
                    coldef[++n] = rdb_tabtok(NULL)

            eof = False
            if n != numcol:
                sys.stderr.write(
                    "RDB file on line %d has %d column names but " +
                    "%d definitions (rdb_cfil)\n", nline, numcol, n
                )
                return -1

            break

            if eof == True:
                return 1
            break

    elif iop == 2:
        # find the next non-comment line
        eof = True
        for namline in infile.readline(maxplin):
            nline += 1
        if line[0] != '#':
            # read the column values
            n = 0
	colval[n] = rdb_tabtok(line)
        colvallen = len(colval)
	while n < colvallen:
            # left-justify the string
            while colval[n] == ' ':
                colval[n] += 1
            # upcase the string
            colval[n] = colval[n].upper()
            # get next token
            colval[++n] = rdb_tabtok(NULL)

        if n != numcol:
            sys.stderr.write(
                "RDB file on line %d has %d values instead of the " +
                "expected %d (rdb_cfil)\n", nline, n, numcol
            )
            return -1
	
        # copy the tokens to the return variables
        if colval[colpos[0]] == '\0':
            cstr2fstr(" ", datatyp, dlen)
	else:
            cstr2fstr(colval[colpos[0]], datatyp, dlen)

        if colval[colpos[1]] == '\0':
            cstr2fstr(" ", rtagny, rlen)
	else:
            cstr2fstr(colval[colpos[1]], rtagny, rlen)

        if colval[colpos[2]] == '\0':
            cstr2fstr(" ", sid, slen)
	else:
            cstr2fstr(colval[colpos[2]], sid, slen)

        if colval[colpos[3]] == '\0':
            cstr2fstr(" ", ddid, ddlen)
	else:
            cstr2fstr(colval[colpos[3]], ddid, ddlen)

        if colval[colpos[4]] == '\0':
            cstr2fstr(" ", stat, tlen)
	else:
            cstr2fstr(colval[colpos[4]], stat, tlen)

        if colval[colpos[5]] == '\0':
            cstr2fstr(" ", begdtm, blen)
	else:
            cstr2fstr(colval[colpos[5]], begdtm, blen)

        if colval[colpos[6]] == '\0':
            cstr2fstr(" ", enddtm, elen)
	else:
            cstr2fstr(colval[colpos[6]], enddtm, elen)

	eof = False

        if eof == True:
            return 1

    elif iop == 3:
        # close the file
        infile.close()

    return 0
