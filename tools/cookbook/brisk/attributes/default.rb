# Needed for the Chef script to function properly
default[:setup][:vanilla_nodes] = 2
default[:setup][:cluster_size] = 4
default[:setup][:current_role] = "brisk"

# A unique name is perferred to stop the risk of 
default[:brisk][:cluster_name] = "Brisk Cluster"

default[:opscenter][:user] = false
default[:opscenter][:pass] = false
default[:opscenter][:free] = false


# Advanced settings
default[:brisk][:initial_token] = 0
default[:brisk][:commitlog_dir] = "/var/lib"
default[:brisk][:data_dir] = "/var/lib"
default[:brisk][:seed] = false
default[:brisk][:rpc_address] = "0.0.0.0"
default[:brisk][:endpoint_snitch] = "org.apache.cassandra.locator.BriskSimpleSnitch"

default[:opscenter][:port] = 8080
default[:opscenter][:interface] = "0.0.0.0"
