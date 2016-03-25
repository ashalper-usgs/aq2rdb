Name:           aq2rdb-client
Version:        1.1.12
Release:        1%{?dist}
Summary:        A command-line program to call the aq2rdb Web service.
Packager:       Andrew Halper <ashalper@usgs.gov>
Vendor:         USGS Office of Water Information
Group:          Applications/Internet
BuildArch:      noarch
Source0:        https://github.com/ashalper-usgs/aq2rdb/%{name}-%{version}.tar.gz
License:        USGS
URL:            https://github.com/ashalper-usgs/aq2rdb
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root
Requires:       python >= 2.6.2-2
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
install -m 755 aq2rdb ${RPM_BUILD_ROOT}%{prefix}/bin

%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root)
%attr(755,root,root) %{prefix}/bin/aq2rdb
%doc

%changelog

* Fri Mar 25 2016 Andrew Halper <ashalper@usgs.gov> 1.1.12-1%{?dist}
- Re-targeted "aq2rdb" Web service reference to cidasdqaasaq2rd.

* Wed Mar 23 2016 Andrew Halper <ashalper@usgs.gov> 1.1.11-1%{?dist}
- Appended newline to final line of usage statement.

* Wed Mar 23 2016 Andrew Halper <ashalper@usgs.gov> 1.1.10-2%{?dist}
- Some minor clean-up of .spec file.

* Wed Mar 23 2016 Andrew Halper <ashalper@usgs.gov> 1.1.10-1%{?dist}
- Initial release.
