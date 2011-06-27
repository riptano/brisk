#
# Cookbook Name:: brisk
# Recipe:: default
#
# Copyright 2011, DataStax
#
# Apache License
#

# knife cookbook upload -a -o cookbooks/ && knife ec2 server create -r "role[brisk]" -I ami-08f40561 --flavor m1.large -S joaquinkey -G HBase-Security -x ubuntu -N server01
# knife cookbook upload -a -o cookbooks/ && knife bootstrap <publicDNS> -r recipe['brisk'] -x ubuntu --sudo


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

    # Add the OpsCenter repo, if user:pass provided
    if node[:opscenter][:user] and node[:opscenter][:pass]
      newSources = "" << "deb http://" << node[:opscenter][:user] << ":" << node[:opscenter][:pass]
      if node[:opscenter][:free]
        newSources << "@deb.opsc.datastax.com/free unstable main"
      else
        newSources << "@deb.opsc.datastax.com/ unstable main"
      end

      file "/etc/apt/sources.list.d/opscenter.list" do
        mode "644"
        content newSources
      end
    end

    # Add the default repo list
    list_source = "maverick.list"
    case node[:platform_version]
      when "10.04"
        list_source = "lucid.list"
      when "10.10"
        list_source = "maverick.list"
      when "11.04"
        list_source = "natty.list"
    end

    cookbook_file "/etc/apt/sources.list.d/brisk.list" do
      source list_source
      mode "0644"
    end

    # Add repo keys and update apt-get
    execute "wget -O - http://debian.datastax.com/debian/repo_key | sudo apt-key add -"
    execute "wget -O - http://opscenter.datastax.com/debian/repo_key | sudo apt-key add -"
    execute "apt-get update"

  when "centos"
    execute "yum clean all" do
      action :nothing
    end

    cookbook_file "/etc/yum.repos.d/datastax.repo" do
      source "datastax.repo"
      mode "0644"
      notifies :run, resources(:execute => "yum clean all"), :immediately
    end
end

###################################################
# 
# Install the Default Packages
# 
###################################################

execute "gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00"
execute "gpg --export --armor 2B5C1B00 | sudo apt-key add -"
execute 'echo "sun-java6-bin shared/accepted-sun-dlj-v1-1 boolean true" | sudo debconf-set-selections'
execute 'sudo add-apt-repository "deb http://archive.canonical.com/ lucid partner"'
execute 'sudo apt-get -y --force-yes update'
execute 'sudo apt-get -y --force-yes install git ant sun-java6-jdk'

execute 'sudo apt-get -y --force-yes upgrade'
execute 'sudo apt-get -y --force-yes install sun-java6-jdk libjna-java htop emacs23-nox sysstat iftop binutils pssh pbzip2 xfsprogs zip unzip ruby openssl libopenssl-ruby curl maven2 ant liblzo2-dev'
execute 'sudo apt-get -y --force-yes --no-install-recommends install mdadm'
execute 'sudo update-alternatives --set java /usr/lib/jvm/java-6-sun/jre/bin/java'
execute 'sudo apt-get remove openjdk-6-jre-headless openjdk-6-jre-lib -y'

execute "clear-data" do
  command "rm -rf /var/lib/cassandra/data/system"
  action :nothing
end

package "brisk-full" do
  notifies :stop, resources(:service => "brisk"), :immediately
  notifies :run, resources(:execute => "clear-data"), :immediately
end

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


###################################################
# 
# Build the Seed List
# 
###################################################

# Helper method for generating tokens
def gen_token node_num
  Chef::Log.info "There's #{node_num+1} brisk nodes."
  node_num * (2 ** 127) / node[:setup][:cluster_size]
end

seeds = []
node[:brisk][:initial_token] = gen_token(brisk_nodes.count - 0) unless node[:brisk][:initial_token] > 0

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
      opscenterdConf = opscenterdConf.gsub(/port =.*/,        "port = #{node[:opscenter][:port]}")
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
