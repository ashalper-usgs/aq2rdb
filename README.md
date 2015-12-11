# aq2rdb
USGS-variant, RDB output from AQUARIUS.

aq2rdb is a Web service, designed to provide some (but not comprehensive) backwards-compatibility features for the NWIS CLI program `nwts2rdb`. The goal of aq2rdb is to ease the transition from CLI `nwts2rdb` and its dependents to the AQUARIUS Web service API.

Presently, aq2rdb provides two Web service "endpoints": `GetDVTable` and `GetUVTable`.

The `GetDVTable` service serves site-referenced, derived statistical "daily values" from AQUARIUS for a given measurement parameter. `GetDVTable` accepts a set of HTTP query field/value pairs, and will respond with an RDB file (a TSV file with some additional specific conventions) containing any daily values found.

The `GetUVTable` service has a similar interface to `GetDVTable`, but serves measured "unit values" (a.k.a. "raw values") instead.