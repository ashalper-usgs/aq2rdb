#! /usr/bin/python
#
#  File - aq2rdb.py
#
#  Purpose - Python emulation of nwts2rdb.
#
#  Author - Andy Halper <ashalper@usgs.gov>
#

import getopt
import sys

# Display usage information for the aq2rdb command
def aq2rdb_usage():
    sys.stderr.write()
    sys.stderr.write("Usage: aq2rdb -ooutfile")
    sys.stderr.write("                -zdbnum")
    sys.stderr.write("                -tdatatype (dv, uv, ms, vt, pk, dc, sv, wl, or qw)")
    sys.stderr.write("                -aagency")
    sys.stderr.write("                -nstation")
    sys.stderr.write("                -dddid (not used with datatypes ms, pk, wl, and qw) OR")
    sys.stderr.write("                -pparm (not used with datatypes ms, pk, and wl) AND")
    sys.stderr.write("                -xloc_nu (only used with datatype vt)")
    sys.stderr.write("                -sstatistic (dv) OR")
    sys.stderr.write("                  uvtype (M)easured, E)dited, R)corrections,")
    sys.stderr.write("                          S)hifts, C)computed, or")
    sys.stderr.write("                          N)Raw Measured (no conversion of input ratings)) OR")
    sys.stderr.write("                  mstype (C)sg, M)eas, D)etailed meas, G)age insp.,")
    sys.stderr.write("                         or Pseudo-UV options:")
    sys.stderr.write("                          1) Discharge,")
    sys.stderr.write("                          2) Stage, or")
    sys.stderr.write("                          3) Velocity)")
    sys.stderr.write("                  vttype (Pseudo-UV options)")
    sys.stderr.write("                          P) Sensor Insp. Primary Reference readings,")
    sys.stderr.write("                          R) Sensor Insp. Primary Recorder readings,")
    sys.stderr.write("                          A) Sensor Insp. All readings,")
    sys.stderr.write("                          M) QW Monitor readings, OR")
    sys.stderr.write("                          F) QW Monitor Field meter readings)")
    sys.stderr.write("                  pktype (F)ull, P)artial. or B)oth) OR")
    sys.stderr.write("                  wlype (1) Water level below land surface,")
    sys.stderr.write("                         2) Water level below measuring point, or,")
    sys.stderr.write("                         3) Water level above sea level)")
    sys.stderr.write("                  qwmeth (QW method code, \"all\" to get all,")
    sys.stderr.write("                          or \"blank\" for blank method)")
    sys.stderr.write("                  (not used with datatypes dc and sv)")
    sys.stderr.write("                -bbegdate (yyyymmdd) (dv, dc, sv, ms, vt, pk) OR")
    sys.stderr.write("                  begdtm (yyyymmddhhmmss) (uv, wl, pseudo-UV ms)")
    sys.stderr.write("                  A value of all zeros indicates beginning of period of record")
    sys.stderr.write("                -eenddate (yyyymmdd) (dv, dc, sv, ms, vt, pk) OR")
    sys.stderr.write("                  enddtm (yyyymmddhhmmss) (uv, wl, pseudo-UV ms)")
    sys.stderr.write("                  A value of all nines indicates end of period of record")
    sys.stderr.write("                -l loctzcd (time zone code or \"LOC\")")
    sys.stderr.write("                -r (rounding suppression)")
    sys.stderr.write("                -w (water year flag)")
    sys.stderr.write("                -c For type \"dv\", Output COMPUTED daily values only")
    sys.stderr.write("                   For other types except pseudo-UV retrievals,")
    sys.stderr.write("                   combine date and time in a single column")
    sys.stderr.write("                -v Make dates and times verbose (excel friendly)")
    sys.stderr.write("                -y transport_cd (Measured Unit-Values only)")
    sys.stderr.write("                    A (ADR), C (CHA), F (FIL), G (EDL), O (OBS),")
    sys.stderr.write("                    P (TEL), R (RAD), S (DCP), U (UNS), or Z (BAK)")
    sys.stderr.write("                    if omitted, defaults to preferred input")
    sys.stderr.write("                -i title_line (Alternate title line if S_STRT is run)")
    sys.stderr.write("       If -o is omitted, writes to stdout AND arguments required ")
    sys.stderr.write("             based on data type as follows:")
    sys.stderr.write("             \"dv\" and \"uv\" requires -t, -n, -s, -b, -e, AND -d OR -p")
    sys.stderr.write("             \"dc\" and \"sv\" requires -t, -n, -b, -e, AND -d or -p")
    sys.stderr.write("             \"ms\" and \"pk\" requires -t, -n, -s, -b, and -e")
    sys.stderr.write("             \"vt\" requires -t, -n, -s, -b, -e AND either -p and -x OR -d")
    sys.stderr.write("             \"wl\" requires -t, -n, -s, -b, and -e")
    sys.stderr.write("             \"qw\" requires -t, -n, -s, -b, -e, and -p")
    sys.stderr.write("       If -o is present, no other arguments are required, and the program")
    sys.stderr.write("             will use ADAPS routines to prompt for them.")
    sys.stderr.write("       If -p is present, -d cannot be present.  The parameter code is")
    sys.stderr.write("             used to find the PRIMARY DD for that parameter.")
    sys.stderr.write("       If -a is omitted, defaults to agency \"USGS\"")
    sys.stderr.write("       If -l is omitted, it will default to \"LOC\"")
    sys.stderr.write("       If -r is present, rounding is suppressed,")
    sys.stderr.write("             otherwise rounded values are output.")
    sys.stderr.write("       If -w is present, -b, and -e will be water years instead,")
    sys.stderr.write("             of dates or datetimes or the user will be prompted")
    sys.stderr.write("             for water years instead of dates or datetimes.")
    sys.stderr.write("       If -c is present and daily-values are being output, only computed")
    sys.stderr.write("             daily values will be retrieved.")
    sys.stderr.write("             For other types combine date and time in a single column")
    sys.stderr.write("       If -z is omitted, defaults to database 1")
    sys.stderr.write("       If -y is present, it is ignored unless Measured Unit-Values")
    sys.stderr.write("             are specified as arguments or selected in the prompting.")
    sys.stderr.write("             If omitted, defaults to preferred input.")
    sys.stderr.write("       If -m is present, it is ignored.")
    sys.stderr.write("       If -i is omitted, the standard S_STRT title line is used.")
    sys.stderr.write("")
    sys.stderr.write("         -- OR --")
    sys.stderr.write("")
    sys.stderr.write("       aq2rdb -fctlfile")
    sys.stderr.write("                -ooutfile")
    sys.stderr.write("                -m (write multiple files)")
    sys.stderr.write("                -zdbnum")
    sys.stderr.write("                -l loctzcd (time zone code or \"LOC\")")
    sys.stderr.write("                -r (rounding suppression)")
    sys.stderr.write("                -c (Output COMPUTED daily values only (DV))")
    sys.stderr.write("                   (combine date and time in a single column (UV))")
    sys.stderr.write("                -v Make dates and times verbose (excel friendly)")
    sys.stderr.write("                -y transport_cd (Measured Unit-Values only)")
    sys.stderr.write("                    A (ADR), C (CHA), F (FIL), G (EDL), O (OBS),")
    sys.stderr.write("                    P (TEL), R (RAD), S (DCP), U (UNS), or Z (BAK)")
    sys.stderr.write("                    if omitted, defaults to preferred input")
    sys.stderr.write("       If -o is omitted, writes to stdout, and -m cannot be used")
    sys.stderr.write("       If -m is present, outfile is used as the output file name prefix.")
    sys.stderr.write("             If omitted, all rows in the control file must be the")
    sys.stderr.write("             same datatype.")
    sys.stderr.write("       If -l is omitted, it will default to \"LOC\"")
    sys.stderr.write("       If -r is present, rounding is suppressed,")
    sys.stderr.write("             otherwise rounded values are output.")
    sys.stderr.write("       If -c is present and daily-values are being output, only computed")
    sys.stderr.write("             daily values will be retrieved.")
    sys.stderr.write("             If unit-values are being output, date and time")
    sys.stderr.write("             are combined into a single datetime column.")
    sys.stderr.write("             This option is ignored if the datatype is not dv or uv.")
    sys.stderr.write("       If -z is omitted, defaults to database 1")
    sys.stderr.write("       If -y is present, it is ignored except for rows in the")
    sys.stderr.write("             control file specifying Measured Unit-Values.")
    sys.stderr.write("             If omitted, defaults to preferred input.")
    sys.stderr.write("       If -t, -a, -n, -d, -p, -s, -b, -e, or -i are present, they are ignored.")
    sys.stderr.write("")
    sys.stderr.write("       The control file (-f argument) is an RDB file containing the")
    sys.stderr.write("       columns \"DATATYPE\", \"AGENCY\", \"STATION\", \"DDID\", \"SUBTYPE\",")
    sys.stderr.write("       \"BEGDATE\", and \"ENDDATE\", corresponding to the -t, -a, -n, -d,")
    sys.stderr.write("       -s, -b, and -e arguments for the no control file case.")
    sys.stderr.write("       For the DDID column, if the datatype is \"qw\" it contains the parameter code.")
    sys.stderr.write("       Otherwise, if the first character of the DDID is a \"P\", then it is treated")
    sys.stderr.write("       as a parameter code and used to locate the PRIMARY DD for that parameter.")
    sys.stderr.write("       All columns must be present, and all columns must also be populated")
    sys.stderr.write("       (not blank and not null), except that DDID is not used (may be blank")
    sys.stderr.write("       or null) when DATATYPE is \"ms\" or \"pk\" and SUBTYPE is not used ")
    sys.stderr.write("       when DATATYPE is \"dc\", or \"sv\".  It does not matter in what ")
    sys.stderr.write("       order the columns appear in the control file.")
    sys.stderr.write("")
    sys.stderr.write("       The control file option cannot be used with pseudo-UV retrievals (all")
    sys.stderr.write("       datatype \"wl\", \"qw\", \"vt\" and some datatype \"ms\" options.")
    sys.stderr.write("")

def main():
    #  FILE *pipe;
    #  char *pipepath;
    #  char temppath[256];
    #  int optchar;
    #  int status;
    #  int error;
    #  char *ddpm;
    #  int i;
    outpath = None
    dbnum = None
    datatyp = None
    agency = None
    station = None
    ddid = None
    parm = None
    stat = None
    begdat = None
    enddat = None
    loc_tz_cd = None
    loc_nu = None
    ctlpath = None
    transport_cd = None
    titlline = None
    rndsup = None
    wyflag = None
    cflag = None
    vflag = None
    multiple = None
    hydra = None
    oblank = " "
    zblank = "1"
    tblank = " "
    ablank = " "
    nblank = " "
    dblank = " "
    pblank = " "
    sblank = " "
    yblank = " "
    iblank = " "
    bblank = " "
    eblank = " "
    lblank = "LOC"
    xblank = "0"
    fblank = " "
    oflag = False
    zflag = False
    tflag = False
    aflag = False
    nflag = False
    dflag = False
    pflag = False
    sflag = False
    yflag = False
    iflag = False
    bflag = False
    eflag = False
    lflag = False
    fflag = False
    xflag = False
    olen = 0
    zlen = 0
    tlen = 0
    alen = 0
    nlen = 0
    dplen = 0
    slen = 0
    ylen = 0
    ilen = 0
    blen = 0
    elen = 0
    llen = 0
    xlen = 0
    flen = 0

    status = 0

    # display usage if no arguments
    if len(sys.argv) <= 1:
        status = 119
    else:
        # get command line arguments 
        rndsup = 'N'
        wyflag = 'N'
        cflag = 'N'
        vflag = 'N'
        multiple = 'N'
        hydra = 'N'

        error = False

        try:
            opts, args = getopt.getopt(
                sys.argv[1:], "o:z:t:a:n:d:p:s:b:e:l:f:y:i:x:rwcvmh"
            )
        except getopt.GetoptError as err:
            # TODO: transplant error-handling code here
            sys.stderr.write("TODO:")

        for opt, arg in opts:
            if opt == "-o":
                oflag = True
                outpath = arg
            elif opt == "-z":
                zflag = True
                dbnum = arg
            elif opt == "-t":
                tflag = True
                datatyp = arg
                # get the length of the data type and convert it to
                # upper case
                tlen = len(datatyp) 
                datatyp = datatyp.upper()
            elif opt == "-a":
                aflag = True
                agency = arg
            elif opt == "-n":
                nflag = True
                station = arg
            elif opt == "-d":
                dflag = True
                ddid = arg
            elif opt == "-p":
                pflag = True
                parm = arg
            elif opt == "-s":
                sflag = True
                stat = arg
            elif opt == "-b":
                bflag = True
                begdat = arg
            elif opt == "-e":
                eflag = True
                enddat = arg
            elif opt == "-l":
                lflag = True
                loc_tz_cd = arg
                # get the length of the time zone code and convert to
                # upper case
                llen = len(loc_tz_cd)
                loc_tz_cd = loc_tz_cd.upper()
            elif opt == "-x":
                xflag = True
                loc_nu = arg
            elif opt == "-r":
                rndsup = 'Y'
            elif opt == "-w":
                wyflag = 'Y'
            elif opt == "-c":
                cflag = 'Y'
            elif opt == "-v":
                vflag = 'Y'
            elif opt == "-m":
                multiple = 'Y'
            elif opt == "-h":
                hydra = 'Y'
            elif opt == "-f":
                fflag = True
                ctlpath = arg
            elif opt == "-y":
                yflag = True
                transport_cd = arg
            elif opt == "-i":
                iflag = True
                titlline = arg
            elif opt == "-?":
                error = True

        if not error:
            # check for argument completion
            # First, processing if -h is specified 

            if hydra == 'Y':
                # date and time must be separate
                cflag = 'N'
                # only -z, -a, -o, -b, and -e can be specified if -h
                # is specified
                if dflag or fflag or iflag or nflag or \
                   pflag or sflag or tflag or yflag or xflag or \
                   wyflag == 'Y' or \
                   cflag == 'Y' or multiple == 'Y':
                    sys.stderr.write()
                    sys.stderr.write(
                        "If -h is specified, only -a, -b, -e, -l, " +
                        "-r, -o, and -z can also be specified"
                    )
                    status = 120
                elif not bflag or not eflag:
                    # both -b and -e must be specified 
                    # date/time range comes from Hydra
                    sys.stderr.write()
                    sys.stderr.write(
                        "If -h is specified, both -b and -e " +
                        "must be specified."
                    )
                    status = 119
                elif not oflag:
                    # -o must be specified if -h is specified
                    sys.stderr.write()
                    sys.stderr.write(
                        "If -h is specified, -o must also be specified."
                    )
                    status = 121
                else:
                    # both -h and -o were specified: outpath is
                    # actually the pathname of a named pipe, save it's
                    # name
                    pipepath = outpath

                    # make sure the named pipe exists
                    if access(pipepath, F_OK):
                        # does not exist - Hydra must have crashed
                        # delete the temp file and exit
                        sys.stderr.write()
                        sys.stderr.write(
                            "Hydra mode - Named pipe does not exist."
                        )
                        status = 125
                    else:
                        # Named pipe exists, create a temporary filename
                        # in outpath
                        tmpnam(temppath)
                        outpath = temppath

        # if a control file is supplied, all arguments are ignored except
        # -z (database number), -r (suppress rounding), -l (local time zone),
        # -m (multiple file output), and -o (output file).  If -o is not 
        # supplied, -m cannot also be supplied - (can't do multiple files if 
        # everything is going to stdout - one file by definition)
        if fflag and multiple == 'Y' and not oflag:
            sys.stderr.write()
            sys.stderr.write(
                "If -f and -m is specified, -o must also " +
                "be specified."
            )
            status = 122

        elif not fflag and dflag and pflag:
            # Not using a control file. Cannot have both -d and -p
            sys.stderr.write()
            sys.stderr.write("Arguments -d and -p cannot both be specified.")
            status = 123
        elif not fflag and not oflag and \
             ( not tflag or not nflag or not bflag or not eflag or \
               ((datatyp != "DC" and datatyp != "SV") and not sflag) or \
               (not datatyp != "QW" and not pflag) or \
               ((datatyp != "MS" and datatyp != "PK" and \
                 datatyp != "WL") and not dflag and not pflag )):
            # Not using a control file, -m is ignored.

            # If -o is not specified, all other arguments must be
            # there as the prompting for missing arguments uses ADAPS
            # subroutines that write the prompts to stdout (they're
            # ports from Prime, remember?) so the RDB output has to go
            # to a file, otherwise the prompts would be mixed in with
            # the RDB output which would make things difficult for a
            # pipeline.
            sys.stderr.write()
            sys.stderr.write(
                "If the -o argument is omitted, then all of -t, -n, -b, -e,"
            )
            sys.stderr.write("and -s if datatype is not \"dc\", or \"sv\"")
            sys.stderr.write("and -p if datatype is \"qw\"")
            sys.stderr.write(
                "and -d or -p if datatype is not \"ms\", \"pk\", " +
                "or \"wl\" must be present."
            )
            status = 124

    if status != 0:
        aq2rdb_usage()
    else:
        # get the length of the output file pathname
        if oflag:
            olen = len(outpath)
        else:
            olen = 1
            outpath = oblank
            # get the length of the database number 
            if zflag:
                zlen = len(dbnum)
            else:
                zlen = 1
                dbnum = zblank
           
        # set data type to blank if not supplied 
        if not tflag:
            tlen = 1
            datatyp = tblank

        if aflag:
            # get the length of the agency code 
            alen = len(agency)
        else:
            alen = 1
            agency = ablank

        # get the length of the station number 
        if nflag:
            nlen = len(station)
        else:
            nlen = 1
            station = nblank

        if dflag:
            # get the length of the DD ID
            dplen = len(ddid)
            # put it in the ddpm variable
            ddpm = ddid
        elif pflag:
            # get the length of the parm code
            dplen = len(parm) + 1
            # put it in the ddpm variable prefixed with "P"
            ddpm = "P" + parm
        else:
            # Neither -d or -p was specified: construct a blank string
            dplen = 1
            ddpm = dblank

        if sflag:
            # get the length of the stat code (or other secondary
            # identifier)
            slen = len(stat)
        else:
            slen = 1
            stat = sblank

        if yflag:
            # get the length of the transport code
            ylen = len(transport_cd)
        else:
            ylen = 1
            transport_cd = yblank

        if iflag:
            # get the length of the title line
            ilen = len(titlline)
        else:
            ilen = 1
            titlline = iblank

        if bflag:
            # get the length of the begin date/time
            blen = len(begdat)
        else:
            blen = 1
            begdat = bblank

        if eflag:
            # get the length of the end date/time 
            elen = len(enddat)
        else:
            elen = 1
            enddat = eblank

        # get the length of the time zone code
        if lflag:
            llen = len(loc_tz_cd)
        else:
            llen = 3
            loc_tz_cd = lblank

        # get the length of the location number
        if xflag:
            xlen = len(loc_nu)
        else:
            xlen = 1
            loc_nu = xblank

        # get the length of the control file pathname 
        if fflag:
            flen = len(ctlpath)
        else:
            flen = 1
            ctlpath = fblank

        status = nwf_rdb_out(ctlpath, multiple, outpath, dbnum,
                             datatyp, rndsup, wyflag, cflag, vflag, hydra,
                             agency, station, ddpm, loc_nu, stat, transport_cd,
                             begdat, enddat, loc_tz_cd, titlline)
        nw_write_error(stderr)
 
        # if Hydra mode, write the temp filename to the named pipe
        if hydra == 'Y':
            # Open the pipe for writing
            pipe = fopen(pipepath, "w")
            if pipe == NULL:
                # Unable to open pipe, delete the temp file and quit
                unlink (outpath)
                sys.stderr.write()
                sys.stderr.write("Cannot open named pipe for writing.")
                status = 126
                # write the temp file pathname to the pipe and close it
                # fprintf (pipe,"reference %s\n",outpath)
                # fclose (pipe)

    sys.exit(status)
