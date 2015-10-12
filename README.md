Dell Storage Center Driver for ClusterHQ/Flocker
================================================

This block storage driver for [Flocker](https://clusterhq.com/) enables Dell Storage Center persistent block storage.

## Overview
Flocker is an open-source Container Data Volume Manager for your Dockerized applications.

Typical Docker data volumes are tied to a single server. With Flocker datasets, the data volume can move with a container between different hosts in your cluster. This flexibility allows stateful container services to access data no matter where the container is placed.

## Prerequisites

The following components are required before using the Dell Storage Center Driver for Flocker:

* A working Flocker installation
* Dell Enterprise Manager 2015 R1 or later
* iSCSI initiator installed on all nodes

**Flocker**

You must first have Flocker installed on your node. Instructions on getting started with Flocker can be found on the [Flocker](https://clusterhq.com/flocker/getting-started) web site.

This driver has been tested against Flocker 1.4.0.

**Enterprise Manager**

You will need Enterprise Manager 2015 R1 or later and a Storage Center array. Enterprise Manager provides
the management interface to provision the block storage on the array.

**iSCSI Initiator**

The current version of this driver only support iSCSI. Please let us know if you are interested in fibre
channel connectivity.

Make sure the iSCSI initiator is installed and configured on each node before attempting to provision
Flocker datasets.

On Red Hat based machines:

```bash
sudo yum install iscsi-initiator-utils
```

On Ubuntu:

```bash
sudo apt-get install open-iscsi
```

## Installation

**Download Driver**

Download the Dell Storage Center driver to the node on which you want to use Storage Center block storage. This process will need to be performed for each node in your cluster.

```bash
git clone https://github.org/dellstorage/storagecenter-flocker-driver
cd storagecenter-flocker-driver
sudo /opt/flocker/bin/python setup.py install
```

**_NOTE:_** Make sure to use the python version installed with Flocker or the driver will not be installed correctly.

**Configure Flocker**

After the Dell Storage Center driver is installed on the local node, Flocker must be configured to use that driver. Connection information is also required for the driver to log in to a Dell Enterprise Manager installation and configure Storage Center volumes.

Configuration is set in Flocker's agent.yml file. Copy the example agent file installed with the driver to get started:

```bash
sudo cp /etc/flocker/example.sc_agent.yml /etc/flocker/agent.yml
sudo vi /etc/flocker/agent.yml
```

Edit the agent.yml file to include the Enterprise Manager and Storage Center information matching that matches your environment. Sample placeholder information is included in the example file.

Some descriptions of the values are:

```bash
version: 1
control-server:
  hostname: "<host or IP of the Flocker control server>"
dataset:
  backend: "dell_storagecenter_driver"
  storage_host: "<host or IP of the Enterprise Manager server>"
  storage_port: "<Enterprise Manager API port. DEFAULT=3033>"
  dell_sc_ssn: <Storage Center system serial number to use>
  username: "<Enterprise Manager user account for the driver>"
  password: "<Enterprise Manager password>"
  volume_folder_name: "<Volume folder for Flocker volumes. DEFAULT='Flocker'>"
  server_folder_name: "<Server folder for Flocker hosts. DEFAULT='Flocker'>"
```

**Test Configuration**

To validate agent settings and make sure everything will work as expected, you may run the following tests from the downloaded driver directory.

```bash
cd storagecenter-flocker-driver
export FLOCKER_CONFIG="/etc/flocker/agent.yml"
sudo trial dell_storagecenter_driver/test_dell_sc.py
```

Several tests will be run to verify the functionality of the driver. Test action logging will output to the file driver.log in the local directory.

## Getting Help
For general Flocker issues, you can either contact [Flocker](http://docs.clusterhq.com/en/latest/gettinginvolved/contributing.html#talk-to-us) or file a [GitHub Issue](https://github.com/clusterhq/flocker/issues).

You can also connect with both ClusterHQ and Dell Storage help on [IRC](https://webchat.freenode.net/) in the \#clusterhq channel.

For specific issues with the Dell Storage Center Driver for Flocker, file a [GitHub Issue](https://github.com/dellstorage/storagecenter_flocker_driver/issues).

If you have any suggestions for an improvements, please feel free create a fork in your repository, make any changes, and submit a pull request to have the changes considered for merging. Community collaboration is welcome!

**As a community project, no warranties are provided for the use of this code.**

## License
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
