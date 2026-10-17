#Requires -RunAsAdministrator
# Lab-only helper: lets a WSL2 Ansible control node reach the Vagrant VMs on 192.168.56.0/24.
# Windows 10 does not forward between the WSL and VMware virtual switches by default, and the
# WSL adapter is recreated at boot, so re-run this after each Windows restart.
wsl.exe -e true | Out-Null   # make sure the WSL adapter exists
$aliases = 'vEthernet (WSL)', 'VMware Network Adapter VMnet2'
Get-NetIPInterface -AddressFamily IPv4 | Where-Object { $aliases -contains $_.InterfaceAlias } | Set-NetIPInterface -Forwarding Enabled
Get-NetIPInterface -AddressFamily IPv4 | Where-Object { $aliases -contains $_.InterfaceAlias } | Format-Table InterfaceAlias, Forwarding -AutoSize
