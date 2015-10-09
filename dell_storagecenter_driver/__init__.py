#    Copyright 2015 Dell Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""Dell Storage Center Plugin for Flocker."""

from flocker import node
from dell_storagecenter_driver import dell_storagecenter_blockdevice


DRIVER_NAME = u"dell_storagecenter_flocker_plugin"


def api_factory(cluster_id, **kwargs):
    """Well known entry point for Flocker to load driver instance."""
    kwargs['cluster_id'] = cluster_id
    return dell_storagecenter_blockdevice.create_driver_instance(
        **kwargs)

FLOCKER_BACKEND = node.BackendDescription(
    # Name isn't actually used for 3rd party plugins
    name=DRIVER_NAME,
    needs_reactor=False,
    needs_cluster_id=True,
    api_factory=api_factory,
    deployer_type=node.DeployerType.block)
