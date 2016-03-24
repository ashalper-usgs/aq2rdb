#! /usr/bin/gawk
#
# File - stat.gawk
#
# Purpose - Reformat NWIS STAT domain table into a JSON version.
#
# Author - Andrew Halper <ashalper@usgs.gov>
#

BEGIN {
    print "{"
}
{
    printf "\"%s\": {\"name\": \"%s\", \"description\": \"%s\"}", $1, $2, $5
    # if this is not the last line in the TSV file
    if (FNR < 3188)
	print ","
}
END {
    printf "\n}"
}
