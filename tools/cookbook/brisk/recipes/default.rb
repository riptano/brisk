#
# Cookbook Name:: brisk
# Recipe:: default
#
# Copyright 2011, DataStax
#
# Apache License
#

###################################################
# 
# Public Variable Declarations
# 
###################################################

service "brisk" do
  action :stop
end

service "opscenterd" do
  action :stop
end

brisk_nodes = search(:node, "role:#{node[:setup][:current_role]}").sort

if node[:opscenter][:user] and node[:opscenter][:pass] and brisk_nodes.count == 0
  installOpscenter = true
else
  installOpscenter = false
end


###################################################
# 
# Setup Repositories
# 
###################################################

case node[:platform]
  when "ubuntu"
    include_recipe "apt"

    # Adds the DataStax repo:
    # deb http://debian.riptano.com/<codename> <codename> main
    apt_repository "datastax-repo" do
      uri "http://debian.datastax.com/" << node['lsb']['codename']
      distribution node['lsb']['codename']
      components ["main"]
      key "http://debian.datastax.com/debian/repo_key"
      action :add
    end

    # Adds the Riptano repo:
    # deb http://riptano.riptano.com/<codename> <codename> main
    apt_repository "riptano-repo" do
      uri "http://debian.riptano.com/" << node['lsb']['codename']
      distribution node['lsb']['codename']
      components ["main"]
      action :add
    end

    # Add the OpsCenter repo, if user:pass provided
    # deb http://<user>:<pass>@deb.opsc.datastax.com/[free] unstable main
    if node[:opscenter][:user] and node[:opscenter][:pass]
      opsCenterURL = "http://" << node[:opscenter][:user] << ":" << node[:opscenter][:pass]
      if node[:opscenter][:free]
        opsCenterURL << "@deb.opsc.datastax.com/free"
      else
        opsCenterURL << "@deb.opsc.datastax.com/"
      end

      apt_repository "opscenter" do
        uri opsCenterURL
        components ["unstable","main"]
        key "http://opscenter.datastax.com/debian/repo_key"
        action :add
      end
    end

    # Adds the Sun Java repo:
    # deb http://archive.canonical.com lucid partner
    apt_repository "sun-java6-jdk" do
      uri "http://archive.canonical.com"
      distribution "lucid"
      components ["partner"]
      action :add
    end

  when "centos", "redhat"
    # Add the DataStax Repo
    platformMajor = node[:platform_version].split(".")[0]
    filename = "/etc/yum.repos.d/datastax.repo"
    repoFile = "[datastax]" << "\n" <<
               "name=DataStax Repo for Apache Cassandra" << "\n" <<
               "baseurl=http://rpm.datastax.com/EL/#{platformMajor}" << "\n" <<
               "enabled=1" << "\n" <<
               "gpgcheck=0" << "\n"
    File.open(filename, 'w') {|f| f.write(repoFile) }

    # Add the OpsCenter Repo, if user:pass provided
    if node[:opscenter][:user] and node[:opscenter][:pass]
      opsCenterURL = "http://" << node[:opscenter][:user] << ":" << node[:opscenter][:pass]
      if node[:opscenter][:free]
        opsCenterURL << "@rpm.opsc.datastax.com/free"
      else
        opsCenterURL << "@rpm.opsc.datastax.com/"
      end

      filename = "/etc/yum.repos.d/opscenter.repo"
      repoFile = "[opscenter]" << "\n" <<
                 "name=DataStax OpsCenter" << "\n" <<
                 "baseurl=" << opsCenterURL << "\n" <<
                 "enabled=1" << "\n" <<
                 "gpgcheck=0" << "\n"
      File.open(filename, 'w') {|f| f.write(repoFile) }
    end

    # Install EPEL (Extra Packages for Enterprise Linux) repository
    platformMajor = node[:platform_version].split(".")[0]
    epelInstalled = File::exists?("/etc/yum.repos.d/epel.repo") or File::exists?("/etc/yum.repos.d/epel-testing.repo")
    if !epelInstalled
      case platformMajor
        when "6"
          execute "sudo rpm -Uvh http://download.fedora.redhat.com/pub/epel/6/#{node[:kernel][:machine]}/epel-release-6-5.noarch.rpm"
        when "5"
          execute "sudo rpm -Uvh http://download.fedora.redhat.com/pub/epel/5/#{node[:kernel][:machine]}/epel-release-5-4.noarch.rpm"
        when "4"
          execute "sudo rpm -Uvh http://download.fedora.redhat.com/pub/epel/4/#{node[:kernel][:machine]}/epel-release-4-10.noarch.rpm"
      end
    end

    execute "yum clean all"
end


###################################################
# 
# Install the Highly Recommended Packages
# 
###################################################

case node[:platform]
  when "ubuntu"
    # Ensure all native components are up to date
    execute 'sudo apt-get -y upgrade'

    # Allow for non-interactive Sun Java setup
    execute 'echo "sun-java6-bin shared/accepted-sun-dlj-v1-1 boolean true" | sudo debconf-set-selections'
    package "sun-java6-jdk"

    # Uninstall other Java Versions
    execute 'sudo update-alternatives --set java /usr/lib/jvm/java-6-sun/jre/bin/java'
    package "openjdk-6-jre-headless" do
      action :remove
    end
    package "openjdk-6-jre-lib" do
      action :remove
    end
    
    # Install JNA and the LZO compressor for Brisk
    package "libjna-java"
    package "liblzo2-dev"
    
  when "centos", "redhat"
    # Ensure all native components are up to date
    execute 'sudo yum -y update'
    execute 'sudo yum -y upgrade'
end


###################################################
# 
# Install Optional Packages
# 
###################################################

# Addtional optional programs/utilities
case node[:platform]
  when "ubuntu"
    package "pssh"
    package "xfsprogs"
    package "maven2"

    # Addtional optional program for RAID management
    package "mdadm" do
      options "--no-install-recommends"
      action :install
    end
  when "centos", "redhat"
    # Addtional optional program for RAID management
    package "mdadm"
end

package "htop"
package "iftop"
package "pbzip2"
package "git"
package "ant"
package "emacs"
package "sysstat"
package "zip"
package "unzip"
package "binutils"
package "ruby"
package "openssl"
package "ant"
package "curl"


###################################################
# 
# Install Brisk [and OpsCenter]
# 
###################################################

execute "clear-data" do
  command "rm -rf /var/lib/cassandra/data/system"
  action :nothing
end

# Install Brisk
package "brisk-full" do
  notifies :stop, resources(:service => "brisk"), :immediately
  notifies :run, resources(:execute => "clear-data"), :immediately
end

# Install Brisk Demos
package "brisk-demos" do
  notifies :stop, resources(:service => "brisk"), :immediately
  notifies :run, resources(:execute => "clear-data"), :immediately
end

# Install OpsCenter
if installOpscenter
  package "opscenter" do
    notifies :stop, resources(:service => "opscenterd"), :immediately
  end
end


###################################################
# 
# Remove the MOTD
# 
###################################################

execute "rm -rf /etc/motd"
execute "touch /etc/motd"


###################################################
# 
# Creating RAID0
# Insert optional personalized RAID code here
# 
###################################################

# A typical setup will want the commit log and data to be on two seperate drives.
# Although for EC2, tests have shown that having the commit log and data on 
# the same RAID0 show better performance.

# mdadm "/dev/md0" do
#   devices [ "/dev/sdb", "/dev/sdc" ]
#   level 0
#   chunk 64
#   action [ :create, :assemble ]
# end

# mount "/raid0/" do
#   device "/dev/md0"
#   fstype "ext3"
# end


###################################################
# 
# Additional Code
# 
###################################################

execute 'echo "export JAVA_HOME=/usr/lib/jvm/java-6-sun" | sudo -E tee -a ~/.bashrc'
execute 'echo "export JAVA_HOME=/usr/lib/jvm/java-6-sun" | sudo -E tee -a ~/.profile'
execute 'sudo bash -c "ulimit -n 32768"'
execute 'echo 1 | sudo tee /proc/sys/vm/overcommit_memory'
execute 'echo "* soft nofile 32768" | sudo tee -a /etc/security/limits.conf'
execute 'echo "* hard nofile 32768" | sudo tee -a /etc/security/limits.conf'

# Open ports for communications in Rackspace.
# This is HORRIBLE security. 
# Make sure to properly configure your cluster here.
if node[:cloud][:provider] == "rackspace" and node[:platform] == "centos"
  execute 'sudo service iptables stop' do
    ignore_failure true
  end
end


###################################################
# 
# Calculate the Token
# 
###################################################

cookbook_file "/tmp/tokentool.py" do
  source "tokentool.py"
  mode "0755"
end

execute "/tmp/tokentool.py #{node[:setup][:vanilla_nodes]} #{node[:setup][:cluster_size] - node[:setup][:vanilla_nodes]} > /tmp/tokens" do
  creates "/tmp/tokens"
end

ruby_block "ReadTokens" do
 block do
  results = []
  open("/tmp/tokens").each do |line|
    results << line.split(':')[1].strip if line.include? 'Node'
  end

  Chef::Log.info results
  node[:brisk][:initial_token] = results[brisk_nodes.count] unless node[:brisk][:initial_token] > 0
 end
end


###################################################
# 
# Build the Seed List
# 
###################################################

seeds = []

# Pull the seeds from the chef db
if brisk_nodes.count == 0
  # Add this node as a seed since this is the first node
  seeds << node[:cloud][:private_ips].first
else
  # Add the first node as a seed
  seeds << brisk_nodes[0][:cloud][:private_ips].first

  # Add the first node in the second DC
  if (brisk_nodes.count > node[:setup][:vanilla_nodes]) and !(node[:setup][:vanilla_nodes] == 0)
    seeds << brisk_nodes[node[:setup][:vanilla_nodes]][:cloud][:private_ips].first
  end

  # Add this node as a seed since this is the first vanilla node
  if brisk_nodes.count == node[:setup][:vanilla_nodes]
    seeds << node[:cloud][:private_ips].first
  end
end


###################################################
# 
# Write Configs and Start Services
# 
###################################################

ruby_block "buildBriskFile" do
  block do
    filename = "/etc/default/brisk"
    briskFile = File.read(filename)
    if brisk_nodes.count < node[:setup][:vanilla_nodes]
      briskFile = briskFile.gsub(/HADOOP_ENABLED=1/, "HADOOP_ENABLED=0")
    else
      briskFile = briskFile.gsub(/HADOOP_ENABLED=0/, "HADOOP_ENABLED=1")
    end
    File.open(filename, 'w') {|f| f.write(briskFile) }
  end
  action :create
  notifies :run, resources(:execute => "clear-data"), :immediately
end

ruby_block "buildCassandraEnv" do
  block do
    filename = "/etc/brisk/cassandra/cassandra-env.sh"
    cassandraEnv = File.read(filename)
    cassandraEnv = cassandraEnv.gsub(/# JVM_OPTS="\$JVM_OPTS -Djava.rmi.server.hostname=<public name>"/, "JVM_OPTS=\"\$JVM_OPTS -Djava.rmi.server.hostname=#{node[:cloud][:private_ips].first}\"")
    File.open(filename, 'w') {|f| f.write(cassandraEnv) }
  end
  action :create
end

ruby_block "buildCassandraYaml" do
  block do
    filename = "/etc/brisk/cassandra/cassandra.yaml"
    cassandraYaml = File.read(filename)
    cassandraYaml = cassandraYaml.gsub(/cluster_name:.*/,               "cluster_name: '#{node[:brisk][:cluster_name]}'")
    cassandraYaml = cassandraYaml.gsub(/initial_token:.*/,              "initial_token: #{node[:brisk][:initial_token]}")
    cassandraYaml = cassandraYaml.gsub(/\/.*\/cassandra\/data/,         "#{node[:brisk][:data_dir]}/cassandra/data")
    cassandraYaml = cassandraYaml.gsub(/\/.*\/cassandra\/commitlog/,    "#{node[:brisk][:commitlog_dir]}/cassandra/commitlog")
    cassandraYaml = cassandraYaml.gsub(/\/.*\/cassandra\/saved_caches/, "#{node[:brisk][:data_dir]}/cassandra/saved_caches")
    cassandraYaml = cassandraYaml.gsub(/seeds:.*/,                      "seeds: \"#{seeds.join(",")}\"")
    cassandraYaml = cassandraYaml.gsub(/listen_address:.*/,             "listen_address: #{node[:cloud][:private_ips].first}")
    cassandraYaml = cassandraYaml.gsub(/rpc_address:.*/,                "rpc_address: #{node[:brisk][:rpc_address]}")
    cassandraYaml = cassandraYaml.gsub(/endpoint_snitch:.*/,            "endpoint_snitch: #{node[:brisk][:endpoint_snitch]}")
    File.open(filename, 'w') {|f| f.write(cassandraYaml) }
  end
  action :create
  notifies :restart, resources(:service => "brisk"), :immediately
end

ruby_block "buildOpscenterdConf" do
  block do
    filename = "/etc/opscenter/opscenterd.conf"
    if File::exists?(filename)
      opscenterdConf = File.read(filename)
      opscenterdConf = opscenterdConf.gsub(/port = 8080/,        "port = #{node[:opscenter][:port]}")
      opscenterdConf = opscenterdConf.gsub(/interface =.*/,   "interface = #{node[:opscenter][:interface]}")
      opscenterdConf = opscenterdConf.gsub(/seed_hosts =.*/,  "seed_hosts = #{node[:cloud][:private_ips].first}")
      Chef::Log.info "Waiting 60 seconds for Brisk to initialize, then start OpsCenter"
      File.open(filename, 'w') {|f| f.write(opscenterdConf) }
      sleep 60
    end
  end
  action :create
  if installOpscenter
    notifies :restart, resources(:service => "opscenterd"), :immediately
  end
end
