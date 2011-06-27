# Quick Start
http://wiki.opscode.com/display/chef/Quick+Start



# Add these lines to ~/.chef/knife.rb
# for Rackspace and EC2 access

# Found at: https://manage.rackspacecloud.com/APIAccess.do
knife[:rackspace_api_username] = "USER"
knife[:rackspace_api_key]  = "KEY"

# Found at: https://aws-portal.amazon.com/gp/aws/developer/account?ie=UTF8&action=access-key
knife[:aws_access_key_id]     = "ID"
knife[:aws_secret_access_key] = "KEY"



# Create a role
knife role create brisk

{
  "name": "brisk",
  "default_attributes": {
  },
  "json_class": "Chef::Role",
  "env_run_lists": {
  },
  "run_list": [
    "recipe[brisk]"
  ],
  "description": "",
  "chef_type": "role",
  "override_attributes": {
  }
}



# Download and place the apt recipe in your cookbooks folder
http://community.opscode.com/cookbooks/apt
# Then run
knife cookbook upload -a -o cookbooks/



# For all scripts below
servername=Joaquin-Chef-Server01

# For EC2 scripts below
pemname=joaquinkey
group=OpenGroup

# Standard Ubuntu (10.10)
knife rackspace server create -r "role[brisk]" -i 69 -f 6 -S $servername -N $servername
knife ec2 server create -r "role[brisk]" -I ami-08f40561 --flavor m1.large -S $pemname -G $group -x ubuntu -N $servername
knife bootstrap -r recipe['brisk'] --sudo -x ubuntu <publicDNS>

# Standard CentOS (5.5)
knife rackspace server create -r "role[brisk]" -i 51 -f 6 -d centos5-gems -S $servername -N $servername
knife bootstrap -r "recipe['brisk']" --sudo -x root -d centos5-gems <publicDNS>

# Standard RHEL
wget http://goo.gl/0k8mV -O- > ~/.chef/bootstrap/rhel5-rbel.erb
knife bootstrap --sudo -x root -d rhel5-rbel <publicDNS>
knife bootstrap -r recipe['brisk'] --sudo -x root -d rhel5-rbel <publicDNS>



# Other useful commands:
knife node show server04 --format json
