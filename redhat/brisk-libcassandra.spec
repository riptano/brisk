%global username cassandra

%define relname %{name}-%{version}
%define cass_name apache-cassandra-%{version}
%define briskname brisk
%define briskversion 0.0.1
%define briskrel %{briskname}-%{briskversion}

Name:           brisk-libcassandra
Version:        0.8.0
Release:        beta2
Summary:        Cassandra is a highly scalable, eventually consistent, distributed, structured key-value store.

Group:          Development/Libraries
License:        Apache Software License
URL:            http://cassandra.apache.org/
Source0:        brisk-src.tar.gz
BuildRoot:      %{_tmppath}/%{relname}-root-%(%{__id_u} -n)

BuildRequires: java-devel
BuildRequires: jpackage-utils
BuildRequires: ant
BuildRequires: ant-nodeps

Conflicts:     cassandra
Conflicts:     apache-cassandra
Obsoletes:     cassandra07

Requires:      java >= 1.6.0
Requires:      jna  >= 3.2.7
Requires:      jpackage-utils
Requires(pre): user(cassandra)
Requires(pre): group(cassandra)
Requires(pre): shadow-utils
Provides:      user(cassandra)
Provides:      group(cassandra)

BuildArch:      noarch

%description
Cassandra brings together the distributed systems technologies from Dynamo
and the data model from Google's BigTable. Like Dynamo, Cassandra is
eventually consistent. Like BigTable, Cassandra provides a ColumnFamily-based
data model richer than typical key/value systems.

For more information see http://cassandra.apache.org/

This distribution of Apache Cassandra is maintained by DataStax, Inc. 
specifically for the purpose of integration with the Brisk platform of 
Apache Hadoop integrations.  

For more information on Brisk, see http://www.datastax.com/brisk

%prep
# tmp hack for now, until we figure out a src target
%setup -q -n brisk
#%setup -q -n %{relname}-src

%build
ant clean jar -Drelease=true

%install
%{__rm} -rf %{buildroot}
mkdir -p %{buildroot}/usr/share/%{username}
mkdir -p %{buildroot}/usr/share/%{briskname}/%{username}
mkdir -p %{buildroot}/usr/share/%{briskname}/%{username}/lib
mkdir -p %{buildroot}/etc/security/limits.d/
mkdir -p %{buildroot}/etc/default
mkdir -p %{buildroot}/etc/brisk/cassandra
mkdir -p %{buildroot}/usr/sbin
mkdir -p %{buildroot}/usr/bin

# copy over configurations and env setup
cp -p resources/%{username}/conf/* %{buildroot}/etc/%{briskname}/%{username}/
cp -p resources/%{username}/lib/*.jar %{buildroot}/usr/share/%{briskname}/%{username}/lib
cp -p build/%{briskrel}.jar %{buildroot}/usr/share/%{briskname}/brisk.jar
cp -p packaging-common/brisk.default %{buildroot}/etc/default/brisk
cp -p packaging-common/%{username}.conf %{buildroot}/etc/security/limits.d/
cp -p packaging-common/%{username}.in.sh %{buildroot}/usr/share/%{username}/

# move cassandra to /usr/sbin
mv resources/%{username}/bin/cassandra %{buildroot}/usr/sbin

# remove collisions and non-linux 
rm resources/%{username}/bin/cassandra.in.sh
rm resources/%{username}/bin/*.bat

# copy the rest to usr 
cp -p resources/%{username}/bin/* %{buildroot}/usr/bin

# Handle the case of interim SNAPHOST builds
cp resources/%{username}/lib/%{cass_name}*jar %{buildroot}/usr/share/%{briskname}/%{username}/lib

# create the storage layout
mkdir -p %{buildroot}/var/lib/%{username}/commitlog
mkdir -p %{buildroot}/var/lib/%{username}/data
mkdir -p %{buildroot}/var/lib/%{username}/saved_caches
mkdir -p %{buildroot}/var/run/%{username}
mkdir -p %{buildroot}/var/log/%{username}

%clean
%{__rm} -rf %{buildroot}

%pre
getent group %{username} >/dev/null || groupadd -r %{username}
getent passwd %{username} >/dev/null || \
useradd -d /usr/share/%{briskname}/%{username} -g %{username} -M -r %{username}
exit 0

%preun
# only delete user on removal, not upgrade
if [ "$1" = "0" ]; then
    userdel %{username}
fi

%files
%defattr(-,root,root,0755)
# is the following needed for ASF compliance?
#%doc CHANGES.txt LICENSE.txt README.txt NEWS.txt NOTICE.txt
%attr(755,root,root) /usr/bin
%attr(755,root,root) /usr/sbin
%attr(755,root,root) %config(noreplace) /etc/default/brisk
%attr(755,root,root) %config(noreplace) /etc/brisk/cassandra
%attr(755,root,root) /etc/security/limits.d/%{username}.conf
# chown on brisk as cassandra is our only user for now
%attr(755,%{username},%{username}) /usr/share/%{briskname}*
%attr(755,%{username},%{username}) /usr/share/%{username}
%attr(755,%{username},%{username}) %config(noreplace) /var/lib/%{username}/*
%attr(755,%{username},%{username}) /var/log/%{username}*
%attr(755,%{username},%{username}) /var/run/%{username}*
