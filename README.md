# PostgreSQL 17 High Availability with Patroni, etcd and PgBouncer (Ansible)

## Quick start
1. `vagrant up` - creates 3 Ubuntu 24.04 VMs (pg1-pg3, 192.168.56.11-13)
2. Create `~/.vault_pass_pg-ha` and `ansible/group_vars/all/vault.yml` (see `vault.yml.example`)
3. `cd ansible && ansible-playbook site.yml`
