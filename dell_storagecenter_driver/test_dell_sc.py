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

"""
Functional tests for
``flocker.node.agents.blockdevice.DellStorageCenterBlockDeviceAPI``
"""
import bitmath
import logging
import os
from uuid import uuid4
import yaml

from flocker.node.agents import blockdevice
from flocker.node.agents.test.test_blockdevice import (
    make_iblockdeviceapi_tests)
from flocker.node.agents.test.test_blockdevice import (
    make_iprofiledblockdeviceapi_tests)
from twisted.python.components import proxyForInterface
from zope.interface import implementer

from dell_storagecenter_driver.dell_storagecenter_blockdevice import (
    create_driver_instance)


MIN_ALLOCATION_SIZE = bitmath.GiB(1).bytes
MIN_ALLOCATION_UNIT = MIN_ALLOCATION_SIZE

LOG = logging.getLogger(__name__)


@implementer(blockdevice.IBlockDeviceAPI, blockdevice.IProfiledBlockDeviceAPI)
class TestDriver(proxyForInterface(blockdevice.IBlockDeviceAPI, 'original')):
    """Wrapper around driver class to provide test cleanup."""
    def __init__(self, original):
        self.original = original
        self.volumes = {}

    def _cleanup(self):
        """Clean up testing artifacts."""
        with self.original._client.open_connection() as api:
            for vol in self.volumes.keys():
                # Make sure it has been cleanly removed
                try:
                    self.original.detach_volume(self.volumes[vol])
                except Exception:
                    pass

                try:
                    api.delete_volume(vol)
                except Exception:
                    LOG.exception('Error cleaning up volume.')

    def create_volume(self, dataset_id, size):
        """Track all volume creation."""
        blockdevvol = self.original.create_volume(dataset_id, size)
        self.volumes[u"%s" % dataset_id] = blockdevvol.blockdevice_id
        return blockdevvol

    def create_volume_with_profile(self, dataset_id, size, profile_name):
        """Track all volume creation."""
        blockdevvol = self.original.create_volume_with_profile(
            dataset_id, size, profile_name)
        self.volumes[u"%s" % dataset_id] = blockdevvol.blockdevice_id
        return blockdevvol


def api_factory(test_case):
    """Create a test instance of the block driver.

    :param test_case: The specific test case instance.
    :return: A test configured driver instance.
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-7s [%(threadName)-19s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        filename='../driver.log')
    test_config_path = os.environ.get(
        'FLOCKER_CONFIG',
        '../example.sc_agent.yml')
    if not os.path.exists(test_config_path):
        raise Exception('Functional test configuration not found.')

    with open(test_config_path) as config_file:
        config = yaml.load(config_file.read())

    config = config.get('dataset', {})
    test_driver = TestDriver(
        create_driver_instance(
            cluster_id=uuid4(),
            **config))
    test_case.addCleanup(test_driver._cleanup)
    return test_driver


class DellStorageCenterBlockDeviceAPIInterfaceTests(
    make_iblockdeviceapi_tests(
        blockdevice_api_factory=(
            lambda test_case: api_factory(test_case)
        ),
        minimum_allocatable_size=MIN_ALLOCATION_SIZE,
        device_allocation_unit=MIN_ALLOCATION_UNIT,
        unknown_blockdevice_id_factory=lambda test: unicode(uuid4()))):
    pass


class DellStorageCenterProfiledBlockDeviceAPIInterfaceTests(
    make_iprofiledblockdeviceapi_tests(
        profiled_blockdevice_api_factory=(
            lambda test_case: api_factory(test_case)
        ),
        dataset_size=MIN_ALLOCATION_UNIT)):
    pass
