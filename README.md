# aq2rdb
USGS-variant, RDB output from AQUARIUS.

aq2rdb is a Web service, designed to provide some (but not comprehensive) backwards-compatibility features for the NWIS CLI program `nwts2rdb`. The goal of aq2rdb is to ease the transition from CLI `nwts2rdb` and its dependents to the AQUARIUS Web service API.

Presently, aq2rdb provides two Web service "endpoints": `GetDVTable` and `GetUVTable`.

The `GetDVTable` service serves site-referenced, derived statistical "daily values" from AQUARIUS for a given measurement parameter. `GetDVTable` accepts a set of HTTP query field/value pairs, and will respond with an RDB file (a TSV file with some additional specific conventions) containing any daily values found.

The `GetUVTable` service has a similar interface to `GetDVTable`, but serves measured "unit values" (a.k.a. "raw values") instead.

## Installing From Source
Clone the aq2rdb repository on GitHub:

    git clone https://github.com/ashalper-usgs/aq2rdb.git

Run the `npmbuild` script in the newly created source directory:

    sh npmbuild
    
This script is safe to run repeatedly, even if you have some or all required npm packages already installed.

## Installing the npm Package

## Running
aq2rdb is started from the Node.js interpreter like this:

    node aq2rdb.js -p port -t aquariusTokenHostname -w waterServicesHostname \
                   -a aquariusHostname --aquariusUserName aquariusUserName --aquariusPassword aquariusPassword

where *port* is the TCP/IP port that aq2rdb will listen on for HTTP requests, *aquariusTokenHostname* is the host name of an aquarius-token server to query for AQUARIUS authentication tokens, *waterServicesHostname* is the host name of a USGS Web Services server to query for site data, and *aquariusHostname* is the host name of an AQUARIUS server to query for time series data.

## Testing
    http://localhost:8081/aq2rdb?p=00060&t=uv&s=C&n=12345678&b=20141001000000&e=20150930000000