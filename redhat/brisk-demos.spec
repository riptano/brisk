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
mkdir -p %{buildroot}/usr/share/brisk-demos/pig/files

cp -p demos/portfolio_manager/portfolio.jar %{buildroot}/usr/share/brisk-demos/portfolio_manager
cp -rp demos/portfolio_manager/bin %{buildroot}/usr/share/brisk-demos/portfolio_manager/
cp -rp demos/portfolio_manager/resources %{buildroot}/usr/share/brisk-demos/portfolio_manager/
cp -rp demos/portfolio_manager/thrift %{buildroot}/usr/share/brisk-demos/portfolio_manager/
cp -rp demos/portfolio_manager/website %{buildroot}/usr/share/brisk-demos/portfolio_manager/
cp -p demos/portfolio_manager/README.txt %{buildroot}/usr/share/brisk-demos/portfolio_manager
cp -p demos/portfolio_manager/10_day_loss.q %{buildroot}/usr/share/brisk-demos/portfolio_manager
cp -p demos/pig/files/example.txt %{buildroot}/usr/share/brisk-demos/pig/files
cp -p demos/pig/001_sort-by-total-cfs.pig %{buildroot}/usr/share/brisk-demos/pig
cp -p demos/pig/002_push-data-to-cassandra.pig %{buildroot}/usr/share/brisk-demos/pig
cp -p demos/pig/003_sort-by-total-cs.pig %{buildroot}/usr/share/brisk-demos/pig
cp -p demos/pig/README.txt %{buildroot}/usr/share/brisk-demos/pig

#cp -pr demos/portfolio_manager %{buildroot}/usr/share/brisk-demos/
#rm -rf %{buildroot}/usr/share/brisk-demos/portfolio_manager/build


%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-,root,root,0755)
%attr(755,%{username},%{username}) %{_datadir}/brisk-demos


