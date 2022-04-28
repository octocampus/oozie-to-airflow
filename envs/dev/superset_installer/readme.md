CONTENTS OF THIS FILE
---------------------

* Introduction
* Requirements
* Recommended modules
* Installation
* Monitoring
* Configuration
* Troubleshooting
* FAQ


INTRODUCTION
------------

This is a  package for installing and configuring apache superset with postgresql as backend database on a
centos machine with celery executor.



* For more information on superset, visit
  [http://superset.apache.org/](http://superset.apache.org/)

* For more information on postgresql, visit
  [http://www.postgresql.org/](http://www.postgresql.org/)

REQUIREMENTS
------------

This module requires the following conditions to be met on target machine:

* [Python 2 or 3(recommanded)](https://www.python.org/downloads/)
* [Ssh with root user allowed](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/v2v_guide/preparation_before_the_p2v_migration-enable_root_login_over_ssh)
* Internet connection

The module has been tested only on CentOS 7 and centos 8 systems.


INSTALLATION
------------

1- First, update the inventory.yml file with the target machine ip address.
```
  hosts: 
    HOST_NAME:  
       ansible_user: root
       ansible_host: IP_ADDRESS

```

2- Update group vars according to your needs

3-  Make sure your Ansible machine has root access via ssh to the target machines.
For documentation on how to do this, please visit the following link:
https://docs.ansible.com/ansible/latest/intro_getting_started.html#intro-getting-started

4- Make sure you have at least python 2.7.9 installed on your machine.
For documentation on how to do this, please visit the following link:
https://docs.python.org/2/install.html

5- Then run the playbook:
```
  ansible-playbook -i inventory.yml install_superset.yml
```

MONITORING
------------

Here is a list of the services that are installed and running on the target machine:
- superset-server.service
- postgresql-10.service

You can start or stop some service with these commands
```
  systemctl start <service-name>
  systemctl restart <service-name>
  systemctl stop <service-name>
```


You can access superset at the following url:
```
  http://IP_ADDRESS:{{ superset_port }}
```


#### NB: Credentials for admin user in superset and postgresql database are configurable in the group vars file.


CONFIGURATION
-------------

* To see more about superset configuration, please visit the following link:
  [http://superset.apache.org/docs/configuration.html](http://superset.apache.org/docs/configuration.html)

* To see more about postgresql configuration, please visit the following link:
  [https://www.postgresql.org/docs/10/runtime-config.html](https://www.postgresql.org/docs/10/runtime-config.html)





TROUBLESHOOTING
---------------

* If some services are not running , please check the journalctl logs.
  For example, check logs via this command.
  ```
  journalctl -u superset-server.service
  ```
  Or restart the  service.
  ```
  systemctl restart airflow-scheduler.service
  ```

FAQ
---


