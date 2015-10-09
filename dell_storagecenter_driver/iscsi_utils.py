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
"""Utility functions for managing local host iSCSI."""

import argparse
from datetime import datetime
import logging
import os
import re
import shlex
import subprocess


LOG = logging.getLogger(__name__)


def get_initiator_name():
    """Gets the iSCSI initiator name."""
    output = subprocess.check_output(
        ['sudo', 'cat', '/etc/iscsi/initiatorname.iscsi'])
    lines = output.split('\n')
    for line in lines:
        if '=' in line:
            parts = line.split('=')
            return parts[1]


def _exec(cmd):
    """Executes a command.

    Runs a command and gets its output.
    :param cmd: The command line to run.
    :returns: The output from the command.
    """
    LOG.info('Running %s', cmd)
    output = subprocess.check_output(shlex.split(cmd))
    LOG.debug('Result: %s', output)
    return output


def _do_login_logout(iqn, ip, do_login):
    """Perform the iSCSI login or logout."""
    try:
        action = "-u"
        if do_login:
            action = "-l"
        _exec('iscsiadm -m node %s -T %s -p %s' %
              (action,
               iqn,
               ip))
        LOG.info('Performed %s to %s at %s', action, iqn, ip)
        return True
    except subprocess.CalledProcessError:
        LOG.info('Error logging in.')
    return False


def _manage_session(ip_addr, port, do_login=True):
    """Manage iSCSI sessions for all ports in a portal."""
    output = _exec('iscsiadm -m discovery -t st -p %s %s' %
                   (ip_addr, port))
    lines = output.split('\n')
    for line in lines:
        if ':' not in line:
            continue
        target = line.split(' ')
        iqn = target[1]
        ip = target[0].split(',')[0]
        _do_login_logout(iqn, ip, do_login)


def iscsi_login(ip_addr, port=3260):
    """Perform an iSCSI login."""
    return _manage_session(ip_addr, port, True)


def iscsi_logout(portal_ip, port=3260):
    """Perform an iSCSI logout."""
    return _manage_session(portal_ip, port, False)


def rescan_iscsi():
    """Perform an iSCSI rescan."""
    start = datetime.now()
    output = _exec('iscsiadm -m session --rescan')
    lines = output.split('\n')
    end = datetime.now()
    LOG.info('Rescan took %s - output: %s', (end - start), lines)


def find_path(device_id):
    """Looks for the local device path.

    Currently only supports a single non-multipath sd device.

    :param device_id: The page 83 device id.
    :returns: The local path or None.
    """
    regex = re.compile('sd[a-z]+(?![\d])')
    for dev in os.listdir('/dev/'):
        if regex.match(dev):
            try:
                output = _exec('/lib/udev/scsi_id --page=0x83 '
                               '--whitelisted --device=/dev/%s' %
                               dev)
                if device_id in output:
                    LOG.info('Found %s at %s', device_id, dev)
                    return '/dev/%s' % dev
            except Exception:
                LOG.exception('Error getting device id for %s', dev)
    return None


def remove_device(path):
    """Prepare removal of SCSI device.

    :param path: The /dev/sdX path to remove.
    """
    if not path:
        return
    sd = path.replace('/dev/', '')
    remove_path = '/sys/block/%s/device/delete' % sd
    if os.path.exists(remove_path):
        try:
            _exec('blockdev --flushbufs %s' % path)
        except Exception:
            LOG.exception('Error flushing IO to %s', path)
        try:
            _exec('sh -c "echo 1 > %s"' % remove_path)
        except Exception:
            LOG.exception('Error removing device %s', sd)


if __name__ == "__main__":
    # Get command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--initiator", "-i", help="Get initiator name.", action='store_true')
    parser.add_argument(
        "--login", "-l", help="Log in to a target.", default=None)
    parser.add_argument(
        "--logout", "-o", help="Log out of a target portal.", default=None)
    parser.add_argument(
        "--rescan", "-r", help="Perform a rescan for device changes.",
        action='store_true')
    parser.add_argument(
        "--remove", "-x", help="Remove a device.", default=None)
    args = parser.parse_args()

    if args.initiator:
        LOG.info(get_initiator_name())

    if args.login:
        iscsi_login(args.login)

    if args.logout:
        iscsi_logout(args.logout)

    if args.rescan:
        rescan_iscsi()

    if args.remove:
        remove_device(args.remove)
