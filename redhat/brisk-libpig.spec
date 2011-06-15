%global username cassandra

%define relname %{name}-%{version}
%define pig_name pig-%{version}
%define briskname brisk

Name:           brisk-libpig
Version:        0.8.3
Release:        1.0~beta2
Summary:        Repackaging of Apache Pig libraries for inclusion in Brisk

Group:          Development/Libraries
License:        Apache Software License
URL:            http://www.datastax.com/products/brisk
Source0:        brisk-src.tar.gz
BuildRoot:      %{_tmppath}/%{relname}-root-%(%{__id_u} -n)

BuildRequires: java-devel
BuildRequires: jpackage-utils
BuildRequires: ant
BuildRequires: ant-nodeps

Requires:      java >= 1.6.0
Requires:      jpackage-utils
Requires(pre): user(cassandra)
Requires(pre): group(cassandra)
Requires(pre): shadow-utils
Provides:      user(cassandra)
Provides:      group(cassandra)

BuildArch:      noarch

%description
Realtime analytics and distributed database (pig libraries) 
Brisk is a realtime analytics system marrying the distributed database
Cassandra and the mapreduce system Hadoop together.
This package contains the Brisk Pig application.

For more information on Brisk, see http://www.datastax.com/brisk

%prep
# tmp hack for now, until we figure out a src target
%setup -q -n brisk
#%setup -q -n %{relname}-src

%build
ant clean jar -Drelease=true

%install
%{__rm} -rf %{buildroot}
mkdir -p %{buildroot}/etc/brisk/pig
mkdir -p %{buildroot}/usr/share/%{briskname}/pig/bin
mkdir -p %{buildroot}/usr/share/%{briskname}/pig/lib
mkdir -p %{buildroot}/var/log/pig


# copy over configurations and libs
cp -p resources/pig/conf/* %{buildroot}/etc/%{briskname}/pig/
cp -pr resources/pig/lib/* %{buildroot}/usr/share/%{briskname}/pig/lib/
cp -rp resources/pig/bin/* %{buildroot}/usr/share/brisk/pig/bin

%clean
%{__rm} -rf %{buildroot}

# still just the user cassandra for now if it does not exist
%pre
getent group %{username} >/dev/null || groupadd -r %{username}
getent passwd %{username} >/dev/null || \
useradd -d /usr/share/%{briskname}/%{username} -g %{username} -M -r %{username}
exit 0

%files
%defattr(-,root,root,0755)
# do we need a %doc task?
%attr(755,%{username},%{username}) %config(noreplace) /etc/%{briskname}/pig

# chown on brisk as cassandra is our only user for now
%attr(755,%{username},%{username}) /usr/share/%{briskname}*
%attr(755,%{username},%{username}) /var/log/pig
