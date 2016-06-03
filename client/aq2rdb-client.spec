Name:           aq2rdb-client
Version:        1.3.0
Release:        1
Summary:        A command-line program to call the aq2rdb Web service.
Packager:       Andrew Halper <ashalper@usgs.gov>
Vendor:         USGS Office of Water Information
Group:          Applications/Internet
BuildArch:      noarch
Source0:        https://github.com/ashalper-usgs/aq2rdb/%{name}-%{version}.tar.gz
Requires:       python >= 2.6.2-2
License:        USGS
URL:            https://github.com/ashalper-usgs/aq2rdb
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root
Prefix:         /usr/local

%description
The aq2rdb client is a command-line program intended to replace the
NWIS program nwts2rdb. The client calls the aq2rdb Web service to
produce RDB files on standard output.

%prep
%setup -q

%build

%install
rm -rf ${RPM_BUILD_ROOT}
mkdir -p ${RPM_BUILD_ROOT}%{prefix}/bin
# only one file in the package
cp aq2rdb ${RPM_BUILD_ROOT}%{prefix}/bin
chmod 755 aq2rdb ${RPM_BUILD_ROOT}%{prefix}/bin/aq2rdb

%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root)
%attr(755,root,root) %{prefix}/bin/aq2rdb
%doc

%changelog

* Fri Jun 03 2016 Andrew Halper <ashalper@usgs.gov> 1.3.0-1
- More coherent "-z" option operation.

* Wed Jun 01 2016 Andrew Halper <ashalper@usgs.gov> 1.2.1-1
- Corrected bug in "-r" option output.

* Thu May 19 2016 Andrew Halper <ashalper@usgs.gov> 1.2.0-2
- Added python-2.6.2-2 RPM to "Requires".

* Fri Apr 29 2016 Andrew Halper <ashalper@usgs.gov> 1.2.0-1
- Added nwts2rdb "-o" option, to save output to a local file.
- Implemented nwts2rdb "-r" (rounding suppression) option.

* Tue Mar 29 2016 Andrew Halper <ashalper@usgs.gov> 1.1.11-2
- Rebuilt on Solaris SPARC because CentOS RPM would not install,
  despite professing to be "noarch".

* Fri Mar 25 2016 Andrew Halper <ashalper@usgs.gov> 1.1.11-1%{?dist}
- Appended newline to final line of usage statement.
- Re-targeted "aq2rdb" Web service reference to cidasdqaasaq2rd.

* Wed Mar 23 2016 Andrew Halper <ashalper@usgs.gov> 1.1.10-2%{?dist}
- Some minor clean-up of .spec file.

* Wed Mar 23 2016 Andrew Halper <ashalper@usgs.gov> 1.1.10-1%{?dist}
- Initial release.
