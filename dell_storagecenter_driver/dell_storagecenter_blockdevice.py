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
"""The Dell Storage Center Block Device Driver."""

import logging
import platform
import threading
import time
import uuid

import bitmath
import eliot
from flocker.node.agents import blockdevice
from twisted.python import filepath
from zope.interface import implementer

import dell_storagecenter_api
import iscsi_utils


LOG = logging.getLogger(__name__)
ALLOCATION_UNIT = bitmath.GiB(1).bytes


class DellStorageCenterBlockDriverLogHandler(logging.Handler):
    """Python log handler to route to Eliot logging."""

    def emit(self, record):
        """Writes log message to the stream.

        :param record: The record to be logged.
        """
        msg = self.format(record)
        eliot.Message.new(
            message_type="flocker:node:agents:blockdevice:dellstoragecenter",
            message_level=record.levelname,
            message=msg).write()


def create_driver_instance(cluster_id, **config):
    """Instantiate a new driver instances.

    Creates a new instance with parameters passed in from the config.
    :param cluster_id: The container cluster ID.
    :param config: The driver configuration settings.
    :return: A new StorageCenterBlockDeviceAPI object.
    """
    # Configure log routing to the Flocker Eliot logging
    root_logger = logging.getLogger()
    root_logger.addHandler(DellStorageCenterBlockDriverLogHandler())
    root_logger.setLevel(logging.DEBUG)

    config['cluster_id'] = cluster_id
    LOG.info("Config passed in: %s", config)
    return DellStorageCenterBlockDeviceAPI(**config)


class BlockDriverAPIException(Exception):
    """General backend API exception."""


@implementer(blockdevice.IBlockDeviceAPI)
class DellStorageCenterBlockDeviceAPI(object):
    """Block device driver for Dell Storage Center.

    Implements the ``IBlockDeviceAPI`` for interacting with Storage Center
    array storage.
    """

    VERSION = '1.0.0'

    def __init__(self, **kwargs):
        """Initialize new instance of the driver.

        :param configuration: The driver configuration settings.
        :param cluster_id: The cluster ID we are running on.
        """
        self.cluster_id = kwargs.get('cluster_id')
        self._local_compute = None
        self.ssn = kwargs.get('dell_sc_ssn', 448)
        self.configuration = kwargs
        self._client = dell_storagecenter_api.StorageCenterApiHelper(
            kwargs)

    def _to_blockdevicevolume(self, scvolume, attached_to=None):
        """Converts our API volume to a ``BlockDeviceVolume``."""
        dataset_id = uuid.UUID('{00000000-0000-0000-0000-000000000000}')
        try:
            dataset_id = uuid.UUID("{%s}" % scvolume.get('name'))
        except ValueError:
            pass
        retval = blockdevice.BlockDeviceVolume(
            blockdevice_id=scvolume.get('name'),
            size=int(
                float(scvolume.get('configuredSize').replace(' Bytes', ''))),
            attached_to=attached_to,
            dataset_id=dataset_id)
        return retval

    def allocation_unit(self):
        """Gets the minimum allocation unit for our backend.

        The Storage Center recommended minimum is 1 GiB.
        :returns: 1 GiB in bytes.
        """
        return ALLOCATION_UNIT

    def compute_instance_id(self):
        """Gets an identifier for this node.

        This will be compared against ``BlockDeviceVolume.attached_to``
        to determine which volumes are locally attached and it will be used
        with ``attach_volume`` to locally attach volumes.

        For Storage Center we use the node's hostname as the identifier.

        :returns: A ``unicode`` object giving a provider-specific node
                  identifier which identifies the node where the method
                  is run.
        """
        if not self._local_compute:
            self._local_compute = unicode(platform.uname()[1])
        return self._local_compute

    def create_volume(self, dataset_id, size):
        """Create a new volume on the array.

        :param dataset_id: The Flocker dataset ID for the volume.
        :param size: The size of the new volume in bytes.
        :return: A ``BlockDeviceVolume``
        """
        volume_name = u"%s" % dataset_id
        volume_size = self._bytes_to_gig(size)

        scvolume = None
        with self._client.open_connection() as api:
            try:
                scvolume = api.create_volume(volume_name,
                                             volume_size)
            except Exception:
                LOG.exception('Error creating volume.')
                raise
        return self._to_blockdevicevolume(scvolume)

    def destroy_volume(self, blockdevice_id):
        """Destroy an existing volume.

        :param blockdevice_id: The volume unique ID.
        """
        deleted = False
        LOG.info('Destroying volume %s', blockdevice_id)
        with self._client.open_connection() as api:
            try:
                volume = api.find_volume(blockdevice_id)
                if not volume:
                    raise blockdevice.UnknownVolume(blockdevice_id)
                deleted = api.delete_volume(blockdevice_id)
            except Exception:
                # TODO(smcginnis) Catch more specific exception
                LOG.exception('Error destroying volume.')
                raise
        if not deleted:
            # Something happened
            raise BlockDriverAPIException('Unable to delete volume.')

    def _do_rescan(self, process):
        """Performs a SCSI rescan on this host."""
        rescan_thread = threading.Thread(target=iscsi_utils.rescan_iscsi)
        rescan_thread.name = '%s_rescan' % process
        rescan_thread.daemon = True
        rescan_thread.start()

    def attach_volume(self, blockdevice_id, attach_to):
        """Attach an existing volume to an initiator.

        :param blockdevice_id: The unique identifier for the volume.
        :param attach_to: An identifier like the one returned by the
            ``compute_instance_id`` method indicating the node to which to
            attach the volume.

        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :returns: A ``BlockDeviceVolume`` with a ``attached_to`` attribute set
            to ``attach_to``.
        """
        LOG.info('Attaching %s to %s', blockdevice_id, attach_to)

        # Functional tests expect a failure if it's already
        # attached, even if we're being asked to attach to
        # the same host.
        # not_local = attach_to != self.compute_instance_id()
        not_local = True

        with self._client.open_connection() as api:
            # Check that we have that volume
            scvolume = api.find_volume(blockdevice_id)
            if not scvolume:
                raise blockdevice.UnknownVolume(blockdevice_id)

            # Make sure we have a server defined for this host
            iqn = iscsi_utils.get_initiator_name()
            host = api.find_server(iqn)
            LOG.info("Search for server returned: %s", host)
            if not host:
                # Try to create a new host
                host = api.create_server(attach_to, iqn)
                LOG.info("Created server %s", host)
            # Make sure the server is logged in to the array
            ports = api.get_iscsi_ports()
            for port in ports:
                iscsi_utils.iscsi_login(port[0], port[1])

            # Make sure we were able to find something
            if not host:
                raise BlockDriverAPIException()

            # First check if we are already mapped
            mappings = api.find_mapping_profiles(scvolume)
            if mappings:
                # See if it is to this server
                if not_local:
                    raise blockdevice.AlreadyAttachedVolume(blockdevice_id)
                for mapping in mappings:
                    if (mapping['server']['instanceName'] !=
                            host['instanceName']):
                        raise blockdevice.AlreadyAttachedVolume(blockdevice_id)

            mapping = api.map_volume(scvolume, host)
            if not mapping:
                raise BlockDriverAPIException(
                    'Unable to map volume to server.')

            self._do_rescan('attach')

            return self._to_blockdevicevolume(scvolume, attach_to)

    def detach_volume(self, blockdevice_id):
        """Detach ``blockdevice_id`` from whatever host it is attached to.

        :param unicode blockdevice_id: The unique identifier for the block
            device being detached.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to anything.
        :returns: ``None``
        """
        LOG.info('Detaching %s', blockdevice_id)

        with self._client.open_connection() as api:
            # Check that we have that volume
            scvolume = api.find_volume(blockdevice_id)
            if not scvolume:
                raise blockdevice.UnknownVolume(blockdevice_id)

            # First check if we are mapped
            mappings = api.find_mapping_profiles(scvolume)
            if not mappings:
                raise blockdevice.UnattachedVolume(blockdevice_id)

            device_id = scvolume['deviceId']
            path = iscsi_utils.find_path(device_id)
            iscsi_utils.remove_device(path)

            # Make sure we have a server defined for this host
            iqn = iscsi_utils.get_initiator_name()
            host = api.find_server(iqn)
            LOG.info("Search for server returned: %s", host)
            if not host:
                # Try to create a new host
                host = api.create_server(
                    self.compute_instance_id(), iqn)
            LOG.info("Created server %s", host)

            # Make sure we were able to find something
            if not host:
                raise BlockDriverAPIException('Unable to locate server.')

            api.unmap_volume(scvolume, host)
        self._do_rescan('detach')

    def list_volumes(self):
        """List all the block devices available via the back end API.
        :returns: A ``list`` of ``BlockDeviceVolume``s.
        """
        volumes = []

        try:
            with self._client.open_connection() as api:
                vols = api.list_volumes()

                # Now convert our API objects to flocker ones
                for vol in vols:
                    attached_to = None
                    mappings = api.find_mapping_profiles(vol)
                    if mappings:
                        attached_to = mappings[0]['server']['instanceName']
                    volumes.append(
                        self._to_blockdevicevolume(vol, attached_to))
        except Exception:
            LOG.exception('Error encountered listing volumes.')
            raise
        LOG.info(volumes)
        return volumes

    def get_device_path(self, blockdevice_id):
        """Return the device path.

        Returns the local device path that has been allocated to the block
        device on the host to which it is currently attached.
        :param unicode blockdevice_id: The unique identifier for the block
            device.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does not
            exist.
        :raises UnattachedVolume: If the supplied ``blockdevice_id`` is
            not attached to a host.
        :returns: A ``FilePath`` for the device.
        """
        device_id = None
        with self._client.open_connection() as api:
            # Check that we have that volume
            volume = api.find_volume(blockdevice_id)
            if not volume:
                raise blockdevice.UnknownVolume(blockdevice_id)

            scvolume = api.find_volume(blockdevice_id)
            device_id = scvolume['deviceId']

            # First check if we are mapped
            # NOTE: The assumption right now is if we are mapped,
            # we are mapped to the local compute host.
            mappings = api.find_mapping_profiles(scvolume)
            if not mappings:
                raise blockdevice.UnattachedVolume(blockdevice_id)

        if not device_id:
            raise blockdevice.UnknownVolume(blockdevice_id)

        # Look for any new devices
        retries = 0
        while retries < 4:
            path = iscsi_utils.find_path(device_id)
            if path:
                return filepath.FilePath(path).realpath()
            retries += 1
            LOG.info('%s not found, attempt %d', device_id, retries)
            time.sleep(5)
        return None

    def resize_volume(self, blockdevice_id, size):
        """Resize an existing volume.

        :param blockdevice_id: The unique identifier for the device.
        :param size: The new requested size.
        :raises UnknownVolume: If the supplied ``blockdevice_id`` does
            not exist.
        :returns: ``None``
        """
        with self._client.open_connection() as api:
            # Check that we have that volume
            scvolume = api.find_volume(blockdevice_id)
            if not scvolume:
                raise blockdevice.UnknownVolume(blockdevice_id)

            volume_size = self._bytes_to_gig(size)
            if not api.expand_volume(scvolume, volume_size):
                raise blockdevice.VolumeException(blockdevice_id)

    def _bytes_to_gig(self, size):
        """Convert size in bytes to GiB.

        :param size: The number of bytes.
        :returns: The size in gigabytes.
        """
        return bitmath.Byte(size).to_GiB().value
