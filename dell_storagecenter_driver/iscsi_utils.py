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
import time


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
    if output:
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
    if ip_addr == '0.0.0.0':
        return
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


def _get_multipath_device(sd_device):
    """Get the multipath device for a volume.

    Output from multipath -l should be something like:
    36000d31000fa9e0000000000000002f2 dm-5 COMPELNT,Compellent Vol
    size=1.0G features='1 queue_if_no_path' hwhandler='0' wp=rw
    `-+- policy='queue-length 0' prio=-1 status=active
      |- 8:0:0:2  sdx 65:112 active undef running
      |- 12:0:0:2 sdv 65:80  active undef running
      |- 14:0:0:2 sdw 65:96  active undef running
      `- 10:0:0:2 sdy 65:128 active undef running

    :param sd_device: The SCSI device to look for.
    :return: The /dev/mapper/ multipath device if one exists.
    """
    result = None
    try:
        output = _exec('multipath -l %s' % sd_device)
        if output:
            lines = output.split('\n')
            for line in lines:
                if 'COMPELNT' not in line:
                    continue
                name = line.split(' ')[0]
                result = '/dev/mapper/%s' % name
                break
    except Exception:
        # Oh well, we tried
        pass

    return result


def find_paths(device_id):
    """Looks for the local device paths.

    Note: The first element will be the multipath device if one is present.

    :param device_id: The page 83 device id.
    :returns: A list of the local paths.
    """
    result = []
    regex = re.compile('sd[a-z]+(?![\d])')
    for dev in os.listdir('/dev/'):
        if regex.match(dev):
            try:
                output = _exec('/lib/udev/scsi_id --page=0x83 '
                               '--whitelisted --device=/dev/%s' %
                               dev)
                if device_id in output:
                    LOG.info('Found %s at %s', device_id, dev)
                    result.append('/dev/%s' % dev)
            except Exception:
                LOG.exception('Error getting device id for %s', dev)

    # Functional tests always want the same device reported
    result.sort()

    if result:
        # Check if there is a multipath device
        mpath_dev = _get_multipath_device(result[0])
        if mpath_dev:
            LOG.info('Found multipath device %s', mpath_dev)
            result.insert(0, mpath_dev)
    return result


def remove_device(path):
    """Prepare removal of SCSI device.

    :param path: The /dev/sdX or /dev/mapper/X path to remove.
    """
    if not path:
        return

    if '/dev/sd' in path:
        sd = path.replace('/dev/', '')
        remove_path = '/sys/block/%s/device/delete' % sd
        if os.path.exists(remove_path):
            try:
                _exec('blockdev --flushbufs %s' % path)
                time.sleep(4)
            except Exception:
                LOG.exception('Error flushing IO to %s', path)
            try:
                _exec('sh -c "echo 1 > %s"' % remove_path)
                time.sleep(1)
            except Exception:
                LOG.exception('Error removing device %s', sd)
    else:
        try:
            path = path.replace('/dev/mapper/', '')
            _exec('multipath -f %s' % path)
        except Exception:
            LOG.exception('Error removing multipath device %s', path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

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
    parser.add_argument(
        "--get_paths", "-g", help="Get paths for a SCSI ID.", default=None)
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

    if args.get_paths:
        paths = find_paths(args.get_paths)
        for path in paths:
            LOG.info(path)
