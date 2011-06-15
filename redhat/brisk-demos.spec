%global username cassandra

%define relname %{name}-%{version}
%define briskname brisk

Name:           brisk-demos
Version:        1.0
Release:        1.0~beta2
Summary:        Demo applicatio for Brisk platform

Group:          Development/Libraries
License:        Apache Software License
URL:            http://www.datastax.com/products/brisk
Source0:        brisk-src.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-root-%(%{__id_u} -n)

BuildRequires: java-devel
BuildRequires: jpackage-utils
BuildRequires: ant
BuildRequires: ant-nodeps

Requires:      brisk-full

BuildArch:     noarch

%description
Realtime analytics and distributed database (cassandra libraries)
Brisk is a realtime analytics system marrying the distributed database
Cassandra and the mapreduce system Hadoop together.

This package contains the Brisk demo application.

Homepage: http://www.datastax.com/products/brisk

%prep
# tmp hack for now, until we figure out a src target
%setup -q -n brisk

%build
ant clean jar -Drelease=true
cd demos/portfolio_manager
ant

%install
mkdir -p %{buildroot}/usr/share/brisk-demos/portfolio_manager

cp -pr demos/portfolio_manager %{buildroot}/usr/share/brisk-demos/
rm -rf %{buildroot}/usr/share/brisk-demos/portfolio_manager/build


%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-,root,root,0755)
%attr(755,%{username},%{username}) %{_datadir}/brisk-demos


