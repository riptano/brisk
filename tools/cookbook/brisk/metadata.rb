maintainer       "DataStax"
maintainer_email "joaquin@datastax.com"
license          "Apache License"
description      "Install and configure Brisk in a multi-node environment"
long_description IO.read(File.join(File.dirname(__FILE__), 'README.rdoc'))
version          "0.1.2"

attribute "setup",
  :display_name => "Setup Configurations",
  :description => "Hash of Setup Configurations",
  :type => "hash"

attribute "setup/vanilla_nodes",
  :display_name => "Number of Vanilla Nodes",
  :description => "Number of nodes that will start up vanilla Cassandra",
  :default => "2"

attribute "setup/cluster_size",
  :display_name => "Cluster Size",
  :description => "Total number of nodes in the cluster",
  :default => "4"

attribute "setup/current_role",
  :display_name => "Current Role Being Assigned",
  :description => "The role that the cluster is being assigned",
  :default => "brisk"




attribute "brisk",
  :display_name => "Brisk",
  :description => "Hash of Brisk attributes",
  :type => "hash"

attribute "brisk/cluster_name",
  :display_name => "Brisk Cluster Name",
  :description => "Keeps clusters together, not allowing servers from other clusters to talk",
  :default => "Brisk Cluster"

attribute "brisk/initial_token",
  :display_name => "Brisk Initial Token",
  :description => "The standard initial token",
  :default => "0"

attribute "brisk/commitlog_dir",
  :display_name => "Brisk Commit Log Directory",                                                                                                                                                                        
  :description => "The location for the commit log (preferably on it's own drive or RAID0 device)",
  :default => "/var/lib"

attribute "brisk/data_dir",
  :display_name => "Brisk Data Directory",                                                                                                                                                                        
  :description => "The location for the data directory (preferably on it's own drive or RAID0 device)",
  :default => "/var/lib"

attribute "brisk/seed",
  :display_name => "Brisk Seed Server",                                                                                                                                                                        
  :description => "This server is the seed for the rest of the cluster",
  :default => "false"

attribute "brisk/rpc_address",
  :display_name => "Brisk RPC Address",                                                                                                                                                                        
  :description => "The address to bind the Thrift RPC service to",
  :default => "0.0.0.0"

attribute "brisk/endpoint_snitch",
  :display_name => "Brisk Endpoint Snitch",                                                                                                                                                                        
  :description => "How Cassandra knows your network topology to route requests efficiently",
  :default => "org.apache.cassandra.locator.BriskSimpleSnitch"




attribute "opscenter",
  :display_name => "OpsCenter",
  :description => "Hash of OpsCenter attributes",
  :type => "hash"

attribute "opscenter/port",
  :display_name => "OpsCenter Port",
  :description => "The port that OpsCenter uses to connect to Cassandra",
  :default => "8080"

attribute "opscenter/interface",
  :display_name => "OpsCenter Interface",
  :description => "The interface accessible via your browser",
  :default => "0.0.0.0"
