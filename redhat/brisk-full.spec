%global username cassandra

%define relname %{name}-%{version}
%define cass_name apache-cassandra-%{version}
%define briskname brisk

Name:           brisk-full
Version:        1.0
Release:        1.0~beta2
Summary:        Meta RPM for full installation of the DataStax Brisk platform

Group:          Development/Libraries
License:        Apache Software License
URL:            http://www.datastax.com/products/brisk
Source0:        brisk-src.tar.gz
BuildRoot:      %{_tmppath}/%{relname}-root-%(%{__id_u} -n)

BuildRequires: java-devel
BuildRequires: jpackage-utils
BuildRequires: ant
BuildRequires: ant-nodeps

Requires:      brisk-libcassandra
Requires:      brisk-libhadoop
Requires:      brisk-libhive
Requires:      brisk-libpig

BuildArch:     noarch

%description
Realtime analytics and distributed database (cassandra libraries)
Brisk is a realtime analytics system marrying the distributed database
Cassandra and the mapreduce system Hadoop together.

This package contains the full Brisk distribution.

Homepage: http://www.datastax.com/products/brisk

%prep
# tmp hack for now, until we figure out a src target
%setup -q -n brisk

%build
ant clean jar -Drelease=true

%install
mkdir -p %{buildroot}/usr/bin/
mkdir -p %{buildroot}/etc/brisk
mkdir -p %{buildroot}/etc/init.d/
mkdir -p %{buildroot}/etc/default/
mkdir -p %{buildroot}/usr/share/brisk/common

cp -p packaging-common/brisk-env.sh %{buildroot}/etc/brisk/
cp -p packaging-common/brisk.in.sh %{buildroot}/usr/share/brisk/
cp -p packaging-common/brisk.default %{buildroot}/etc/default/brisk
cp -p resources/brisk/lib/*.jar %{buildroot}/usr/share/brisk/common
cp -p redhat/brisk %{buildroot}/etc/init.d/
cp -p bin/brisk %{buildroot}/usr/bin/
cp -p bin/brisktool %{buildroot}/usr/bin/

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-,root,root,0755)
%attr(755,root,root) %config /etc/brisk
%attr(755,root,root) %config /etc/default/brisk
%attr(755,root,root) /etc/init.d/brisk
%attr(755,root,root) /usr/share/brisk/common
%attr(755,root,root) %{_bindir}/brisk
%attr(755,root,root) %{_bindir}/brisktool

%attr(755,%{username},%{username}) /usr/share/brisk/brisk.in.sh
