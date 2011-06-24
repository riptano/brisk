case node[:platform]
when "ubuntu"
  execute "apt-get update" do
	action :nothing
  end

  execute "get-key" do
	command "wget -O - http://debian.datastax.com/debian/repo_key | sudo apt-key add -"
	action :nothing
  end

  list_source = "maverick.list"
  case node[:platform_version]
  when "10.04"
	list_source = "lucid.list"
  when "10.10"
	list_source = "maverick.list"
  end

  cookbook_file "/etc/apt/sources.list.d/brisk.list" do
	source list_source
	mode "0644"
	notifies :run, resources(:execute => "get-key"), :immediately
	notifies :run, "execute[apt-get update]", :immediately
  end
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

service "brisk" do
  action :nothing
end

execute "clear-data" do
  command "rm -rf /var/lib/cassandra/data/system"
  action :nothing
end

package "brisk-full" do
  notifies :stop, resources(:service => "brisk"), :immediately
  notifies :run, resources(:execute => "clear-data"), :immediately
end

cookbook_file "/etc/default/brisk" do
  source "brisk.default"
  mode "0644"
  notifies :restart, "service[brisk]"
end

def gen_token node_num
  Chef::Log.info "There's #{node_num+1} brisk nodes."
  node_num * (2 ** 127) / node[:brisk][:max_num]
end

seeds = []
brisk_nodes = search(:node, "role:brisk")
# Here we're looking to see how many nodes there are
# If we're the only one, we become a seed
node[:brisk][:initial_token] = gen_token(brisk_nodes.count - 0) unless node[:brisk][:initial_token] > 0
node[:brisk][:seed] = true unless brisk_nodes.count > 1

seeds << node[:cloud][:private_ips].first if node[:brisk][:seed]
brisk_nodes.each do |n|
  next if n.name == node.name
  seeds << n[:cloud][:private_ips].first if n[:brisk][:seed]
end

template "/etc/brisk/cassandra/cassandra.yaml" do
  mode "0644"
  notifies :restart, "service[brisk]"
  variables ({ :seeds => seeds })
end
