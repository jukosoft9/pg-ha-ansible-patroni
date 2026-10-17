# -*- mode: ruby -*-
# Creates 3 bare Ubuntu 24.04 VMs for the PostgreSQL + Patroni HA cluster.
# Vagrant ONLY provisions infrastructure (VM, network, SSH access).
# All software configuration is done by Ansible (see ansible/).

NODES = {
  "pg1" => "192.168.56.11",
  "pg2" => "192.168.56.12",
  "pg3" => "192.168.56.13",
}
BOX       = "bento/ubuntu-24.04"
CPUS      = (ENV["PG_VM_CPUS"]   || 2).to_i
MEMORY_MB = (ENV["PG_VM_MEMORY"] || 2560).to_i

# Host side of the private network, and the subnet of an Ansible control node that
# sits behind the host's NAT (e.g. WSL2 on Windows). Set PG_CONTROL_SUBNET="" to skip.
HOST_GATEWAY   = ENV.fetch("PG_HOST_GATEWAY", "192.168.56.1")
CONTROL_SUBNET = ENV.fetch("PG_CONTROL_SUBNET", "172.16.0.0/12")

PUBKEY_PATH = File.expand_path("keys/ansible.pub", File.dirname(__FILE__))
unless File.exist?(PUBKEY_PATH)
  abort "Missing #{PUBKEY_PATH} - copy your Ansible control node's public key there (see README)."
end
ANSIBLE_PUBKEY = File.read(PUBKEY_PATH).strip

Vagrant.configure("2") do |config|
  config.vm.box          = BOX
  config.vm.boot_timeout = 600
  config.vm.synced_folder ".", "/vagrant", disabled: true

  # Primary provider: VMware Workstation
  config.vm.provider "vmware_desktop" do |v|
    v.gui             = false
    v.linked_clone    = true
    v.vmx["memsize"]  = MEMORY_MB.to_s
    v.vmx["numvcpus"] = CPUS.to_s
    # Pin NIC PCI slots so interface names (eth0/eth1) stay stable across Vagrant versions
    v.vmx["ethernet0.pcislotnumber"] = "160"
    v.vmx["ethernet1.pcislotnumber"] = "224"
  end

  # Alternative provider: VirtualBox (same box, same topology)
  config.vm.provider "virtualbox" do |vb|
    vb.linked_clone = true
    vb.memory       = MEMORY_MB
    vb.cpus         = CPUS
  end

  NODES.each do |name, ip|
    config.vm.define name do |node|
      node.vm.hostname = name
      node.vm.network "private_network", ip: ip
    end
  end

  # Authorise the Ansible control node's public key for the vagrant user (idempotent).
  config.vm.provision "shell", name: "ansible-key", inline: <<~SHELL
    install -d -m 700 -o vagrant -g vagrant /home/vagrant/.ssh
    touch /home/vagrant/.ssh/authorized_keys
    grep -qxF '#{ANSIBLE_PUBKEY}' /home/vagrant/.ssh/authorized_keys || echo '#{ANSIBLE_PUBKEY}' >> /home/vagrant/.ssh/authorized_keys
    chown vagrant:vagrant /home/vagrant/.ssh/authorized_keys
    chmod 600 /home/vagrant/.ssh/authorized_keys
  SHELL

  # Persistent return route so replies to a NATed control node (WSL2) go back via the host,
  # not out of the VMware NAT adapter (eth0). 'ip route replace' keeps it idempotent.
  unless CONTROL_SUBNET.empty?
    config.vm.provision "shell", name: "control-node-route", inline: <<~SHELL
      cat > /etc/systemd/system/control-node-route.service <<'UNIT'
      [Unit]
      Description=Return route to the Ansible control node subnet via the host
      After=network-online.target
      Wants=network-online.target

      [Service]
      Type=oneshot
      ExecStart=/usr/sbin/ip route replace #{CONTROL_SUBNET} via #{HOST_GATEWAY} dev eth1
      RemainAfterExit=yes

      [Install]
      WantedBy=multi-user.target
      UNIT
      systemctl daemon-reload
      systemctl enable --now control-node-route.service
    SHELL
  end
end
