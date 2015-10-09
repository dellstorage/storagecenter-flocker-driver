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
'''Interface for interacting with the Dell Storage Center array.'''

import json
import logging
import os.path
import requests
import six


DEFAULT_VOLUME_FOLDER = 'Flocker'
DEFAULT_SERVER_FOLDER = 'Flocker'
LOG = logging.getLogger(__name__)


class PayloadFilter(object):
    """Storage Center REST API filtering structure.

    Simple class for creating filters for interacting with the Dell
    Storage API on EM2015R1 and later.
    """

    def __init__(self, filtertype='AND'):
        self.payload = {}
        self.payload['filter'] = {'filterType': filtertype,
                                  'filters': []}

    def append(self, name, val, filtertype='Equals'):
        """Add a filter value.

        :param name: Name of the filter.
        :param val: Value of the filter.
        :param filtertype: The type of filtering operation to perform.
        """
        if val is not None:
            apifilter = {}
            apifilter['attributeName'] = name
            apifilter['attributeValue'] = val
            apifilter['filterType'] = filtertype
            self.payload['filter']['filters'].append(apifilter)


class LegacyPayloadFilter(object):
    """Storage Center REST API filtering structure.

    Simple class for creating filters for interacting with the Dell
    Storage API pre EM2015R1.
    """

    def __init__(self, filter_type='AND'):
        self.payload = {'filterType': filter_type,
                        'filters': []}

    def append(self, name, val, filtertype='Equals'):
        """Add a filter value.

        :param name: Name of the filter.
        :param val: Value of the filter.
        :param filtertype: The type of filtering operation to perform.
        """
        if val is not None:
            apifilter = {}
            apifilter['attributeName'] = name
            apifilter['attributeValue'] = val
            apifilter['filterType'] = filtertype
            self.payload['filters'].append(apifilter)


class HttpClient(object):
    """Wrapper class for making Storage Center API calls."""

    def __init__(self, host, port, user, password, verify):
        """HttpClient handles the REST requests.

        :param host: IP address of the Dell Data Collector.
        :param port: Port the Data Collector is listening on.
        :param user: User account to login with.
        :param password: Password.
        :param verify: Boolean indicating whether certificate verification
                       should be turned on or not.
        """
        self.base_url = 'https://%s:%s/api/rest/' % (host, port)
        self.session = requests.Session()
        self.session.auth = (user, password)
        self.header = {}
        self.header['Content-Type'] = 'application/json; charset=utf-8'
        self.header['x-dell-api-version'] = '2.0'
        self.verify = verify

        if not verify:
            requests.packages.urllib3.disable_warnings()

    def __enter__(self):
        return self

    def __exit__(self, tipe, value, traceback):
        self.session.close()

    def _format_url(self, url):
        """Formats the REST URL to use for API calls."""
        return '%s%s' % (self.base_url, url if url[0] != '/' else url[1:])

    def get(self, url):
        """Perform a REST GET request."""
        return self.session.get(
            self._format_url(url),
            headers=self.header,
            verify=self.verify)

    def post(self, url, payload):
        """Perform a REST POST request."""
        return self.session.post(
            self._format_url(url),
            data=json.dumps(payload,
                            ensure_ascii=False).encode('utf-8'),
            headers=self.header,
            verify=self.verify)

    def put(self, url, payload):
        """Perform a REST PUT request."""
        return self.session.put(
            self._format_url(url),
            data=json.dumps(payload,
                            ensure_ascii=False).encode('utf-8'),
            headers=self.header,
            verify=self.verify)

    def delete(self, url):
        """Perform a REST DELETE request."""
        return self.session.delete(
            self._format_url(url),
            headers=self.header,
            verify=self.verify)


class StorageCenterApiHelper(object):
    """Helper class for working with the SC API.

    Helper class for API access.  Handles opening and closing the
    connection to the Dell Enterprise Manager.
    """
    def __init__(self, config):
        self.config = config
        LOG.error("Config: %s", config)

    def open_connection(self):
        """Creates the StorageCenterApi object.

        :return: StorageCenterApi object.
        :raises: VolumeBackendAPIException
        """
        connection = StorageCenterApi(self.config['storage_host'],
                                      self.config.get('storage_port', 3033),
                                      self.config['username'],
                                      self.config['password'],
                                      False)
        connection.ssn = self.config['dell_sc_ssn']
        connection.vfname = self.config.get(
            'volume_folder_name', DEFAULT_VOLUME_FOLDER)
        connection.sfname = self.config.get(
            'server_folder_name', DEFAULT_SERVER_FOLDER)
        connection.open_connection()
        return connection


class StorageCenterApi(object):
    """Storage Center API interface.

    Handles calls to Dell Enterprise Manager (EM) via the REST API interface.
    """
    APIVERSION = '2.3.1'

    def __init__(self, host, port, user, password, verify):
        """This creates a connection to Dell Enterprise Manager.

        :param host: IP address of the Dell Data Collector.
        :param port: Port the Data Collector is listening on.
        :param user: User account to login with.
        :param password: Password.
        :param verify: Boolean indicating whether certificate verification
                       should be turned on or not.
        """
        self.notes = 'Created by Dell Flocker Driver'
        self.ssn = None
        self.vfname = DEFAULT_VOLUME_FOLDER
        self.sfname = DEFAULT_SERVER_FOLDER
        self.legacypayloadfilters = True
        self.client = HttpClient(host,
                                 port,
                                 user,
                                 password,
                                 verify)

    def __enter__(self):
        return self

    def __exit__(self, tipe, value, traceback):
        self.close_connection()

    def _check_result(self, rest_response):
        """Checks and logs API responses.

        :param rest_response: The result from a REST API call.
        :returns: ``True`` if success, ``False`` otherwise.
        """
        if 200 <= rest_response.status_code < 300:
            # API call was a normal success
            return True

        LOG.debug('REST call result:\n'
                  '\tCode:   %(code)d\n'
                  '\tReason: %(reason)s\n'
                  '\tText:   %(text)s',
                  {'code': rest_response.status_code,
                   'reason': rest_response.reason,
                   'text': rest_response.text})
        return False

    def _path_to_array(self, path):
        """Breaks a path into a reversed string array.

        :param path: Path to a folder on the Storage Center.
        :return: A reversed array of each path element.
        """
        array = []
        while True:
            (path, tail) = os.path.split(path)
            if tail == '':
                array.reverse()
                return array
            array.append(tail)

    def _first_result(self, blob):
        """Get the first result from the JSON return value.

        :param blob: Full return from a REST call.
        :return: The JSON encoded dict or the first item in a JSON encoded
                 list.
        """
        return self._get_result(blob, None, None)

    def _get_result(self, blob, attribute, value):
        """Find the result specified by attribute and value.

        If the JSON blob is a list then it will be searched for the attribute
        and value combination.  If attribute and value are not specified then
        the the first item is returned.  If the JSON blob is a dict then it
        will be returned so long as the dict matches the attribute and value
        combination or attribute is None.

        :param blob: The REST call's JSON response.  Can be a list or dict.
        :param attribute: The attribute we are looking for.  If it is None
                          the first item in the list, or the dict, is returned.
        :param value: The attribute value we are looking for.  If the attribute
                      is None this value is ignored.
        :returns: The JSON content in blob, the dict specified by matching the
                  attribute and value or None.
        """
        rsp = None
        content = self._get_json(blob)
        if content is not None:
            # We can get a list or a dict or nothing
            if isinstance(content, list):
                for r in content:
                    if attribute is None or r.get(attribute) == value:
                        rsp = r
                        break
            elif isinstance(content, dict):
                if attribute is None or content.get(attribute) == value:
                    rsp = content
            elif attribute is None:
                rsp = content

        if rsp is None:
            LOG.debug('Unable to find result where %(attr)s is %(val)s',
                      {'attr': attribute,
                       'val': value})
            LOG.debug('Blob was %(blob)s', {'blob': blob.text})
        return rsp

    def _get_json(self, blob):
        """Returns a dict from the JSON of a REST response.

        :param blob: The response from a REST call.
        :returns: JSON or None on error.
        """
        try:
            return blob.json()
        except AttributeError:
            LOG.error('Error invalid json: %s',
                      blob)
        return None

    def _get_id(self, blob):
        """Returns the instanceId from a Dell REST object.

        :param blob: A Dell SC REST call's response.
        :returns: The instanceId from the Dell SC object or None on error.
        """
        try:
            if isinstance(blob, dict):
                return blob.get('instanceId')
        except AttributeError:
            LOG.error('Invalid API object: %s',
                      blob)
        return None

    def _get_payload_filter(self, filter_type='AND'):
        """Gets the appropriate payload filter.

        Prior to 2.2 we need to use the LegacyPayloadFilters.
        :param filter_type: The type of filter to create.
        :returns: The right filter version for the API version.
        """
        if self.legacypayloadfilters:
            return LegacyPayloadFilter(filter_type)
        return PayloadFilter(filter_type)

    def open_connection(self):
        """Authenticate against Dell Enterprise Manager.

        :raises: VolumeBackendAPIException.
        """

        payload = {}
        payload['Application'] = 'Flocker REST Driver'
        payload['ApplicationVersion'] = self.APIVERSION
        r = self.client.post('ApiConnection/Login',
                             payload)

        if not self._check_result(r):
            raise Exception('Failed to connect to Enterprise Manager.')

        # We should be logged in.  Try to grab the api version out of the
        # response.
        try:
            apidict = self._get_json(r)
            version = apidict['apiVersion']
            splitver = version.split('.')
            if splitver[0] >= '2':
                self.legacypayloadfilters = True
        except Exception:
            # Good return but not the login response we were expecting.
            # Log it and error out.
            LOG.error('Unrecognized Login Response: %s', r)

    def close_connection(self):
        """Logout of Dell Enterprise Manager."""
        r = self.client.post('ApiConnection/Logout',
                             {})
        # Not much we can do if this somehow fails, just log it
        self._check_result(r)
        self.client = None

    def find_sc(self):
        """Check that the SC is there and being managed by EM.

        :returns: The SC SSN.
        :raises: VolumeBackendAPIException
        """
        r = self.client.get('StorageCenter/StorageCenter')
        result = self._get_result(r,
                                  'scSerialNumber',
                                  self.ssn)
        if result is None:
            LOG.error('Failed to find %(s)s. Result %(r)s',
                      {'s': self.ssn,
                       'r': r})
            raise Exception('Failed to find Storage Center')

        return self._get_id(result)

    # Folder functions

    def _create_folder(self, url, parent, folder):
        """Creates folder under parent.

        This can create both to server and volume folders.  The REST url
        sent in defines the folder type being created on the Dell Storage
        Center backend.

        :param url: This is the Dell SC rest url for creating the specific
                    (server or volume) folder type.
        :param parent: The instance ID of this folder's parent folder.
        :param folder: The folder name to be created.  This is one level deep.
        :returns: The REST folder object.
        """
        scfolder = None
        payload = {}
        payload['Name'] = folder
        payload['StorageCenter'] = self.ssn
        if parent != '':
            payload['Parent'] = parent
        payload['Notes'] = self.notes

        r = self.client.post(url,
                             payload)
        if r.status_code != 201:
            LOG.debug('%(url)s error: %(code)d %(reason)s',
                      {'url': url,
                       'code': r.status_code,
                       'reason': r.reason})
        else:
            scfolder = self._first_result(r)
        return scfolder

    def _create_folder_path(self, url, foldername):
        """Creates a folder path from a fully qualified name.

        The REST url sent in defines the folder type being created on the Dell
        Storage Center backend.  Thus this is generic to server and volume
        folders.

        :param url: This is the Dell SC REST url for creating the specific
                    (server or volume) folder type.
        :param foldername: The full folder name with path.
        :returns: The REST folder object.
        """
        path = self._path_to_array(foldername)
        folderpath = ''
        instanceId = ''
        # Technically the first folder is the root so that is already created.
        found = True
        scfolder = None
        for folder in path:
            folderpath = folderpath + folder
            # If the last was found see if this part of the path exists too
            if found:
                listurl = url + '/GetList'
                scfolder = self._find_folder(listurl,
                                             folderpath)
                if not scfolder:
                    found = False
            # We didn't find it so create it
            if not found:
                scfolder = self._create_folder(url,
                                               instanceId,
                                               folder)
            # If we haven't found a folder or created it then leave
            if not scfolder:
                LOG.error('Unable to create folder path %s',
                          folderpath)
                break
            # Next part of the path will need this
            instanceId = self._get_id(scfolder)
            folderpath = folderpath + '/'
        return scfolder

    def _find_folder(self, url, foldername):
        """Find a folder on the SC using the specified url.

        Most of the time the folder will already have been created so
        we look for the end folder and check that the rest of the path is
        right.

        The REST url sent in defines the folder type being created on the Dell
        Storage Center backend.  Thus this is generic to server and volume
        folders.

        :param url: The portion of the url after the base url (see http class)
                    to use for this operation.  (Can be for Server or Volume
                    folders.)
        :param foldername: Full path to the folder we are looking for.
        :returns: Dell folder object.
        """
        pf = self._get_payload_filter()
        pf.append('scSerialNumber', self.ssn)
        basename = os.path.basename(foldername)
        pf.append('Name', basename)
        # If we have any kind of path we throw it into the filters.
        folderpath = os.path.dirname(foldername)
        if folderpath != '':
            # SC convention is to end with a '/' so make sure we do.
            folderpath += '/'
            pf.append('folderPath', folderpath)
        folder = None
        r = self.client.post(url,
                             pf.payload)
        if self._check_result(r):
            folder = self._get_result(r,
                                      'folderPath',
                                      folderpath)
        return folder

    def _find_volume_folder(self, create=False):
        """Looks for the volume folder where backend volumes will be created.

        Volume folder is specified in the config. See __init__.

        :param create: If True will create the folder if not found.
        :returns: Folder object.
        """
        folder = self._find_folder('StorageCenter/ScVolumeFolder/GetList',
                                   self.vfname)
        # Doesn't exist?  make it
        if folder is None and create is True:
            LOG.info('Need to create folder %s', self.vfname)
            folder = self._create_folder_path('StorageCenter/ScVolumeFolder',
                                              self.vfname)
        return folder

    def _init_volume(self, scvolume):
        """Initializes the volume.

        Maps the volume to a random server and immediately unmaps
        it. This initializes the volume.

        Don't wig out if this fails.
        :param scvolume: Dell Volume object.
        """
        pf = self._get_payload_filter()
        pf.append('scSerialNumber', scvolume.get('scSerialNumber'), 'Equals')
        r = self.client.post('StorageCenter/ScServer/GetList', pf.payload)
        if r.status_code == 200:
            scservers = self._get_json(r)
            # Sort through the servers looking for one with connectivity.
            for scserver in scservers:
                # TODO(tom_swanson): Add check for server type.
                # This needs to be either a physical or virtual server.
                # Outside of tests this should not matter as we only
                # "init" a volume to allow snapshotting of an empty volume.
                if scserver.get('status', '').lower() != 'down':
                    # Map to actually create the volume
                    self.map_volume(scvolume,
                                    scserver)
                    # We have changed the volume so grab a new copy of it.
                    scvolume = self.find_volume(scvolume.get('name'))
                    self.unmap_volume(scvolume,
                                      scserver)
                    return
        # We didn't map/unmap the volume.  So no initialization done.
        # Warn the user before we leave.  Note that this is almost certainly
        # a tempest test failure we are trying to catch here.  A snapshot
        # has likely been attempted before the volume has been instantiated
        # on the Storage Center.  In the real world no one will snapshot
        # a volume without first putting some data in that volume.
        LOG.warning('Volume initialization failure. (%s)',
                    self._get_id(scvolume))

    def _find_storage_profile(self, storage_profile):
        """Looks for a Storage Profile on the array.

        Storage Profiles determine tiering settings. If not specified a volume
        will use the Default storage profile.

        :param storage_profile: The Storage Profile name to find with any
                                spaces stripped.
        :returns: The Storage Profile object or None.
        """
        if not storage_profile:
            return None

        # Since we are stripping out spaces for convenience we are not
        # able to just filter on name. Need to get all Storage Profiles
        # and look through for the one we want. Never many profiles, so
        # this doesn't cause as much overhead as it might seem.
        storage_profile = storage_profile.replace(' ', '').lower()
        pf = self._get_payload_filter()
        pf.append('scSerialNumber', self.ssn, 'Equals')
        r = self.client.post(
            'StorageCenter/ScStorageProfile/GetList', pf.payload)
        if self._check_result(r):
            profiles = self._get_json(r)
            for profile in profiles:
                # Look for the stripped, case insensitive match
                name = profile.get('name', '').replace(' ', '').lower()
                if name == storage_profile:
                    return profile
        return None

    def list_volumes(self):
        """Gets all volumes in our configured folder.

        :returns: All volumes present in the volume folder.
        """
        LOG.debug('Getting list of all volumes.')
        result = []

        # Make sure our volume folder is created.
        volume_folder = self._find_volume_folder(create=True)
        if not volume_folder:
            LOG.error("Error getting configured volume folder.")
            return result

        # Query the array for all volumes in the folder
        pf = self._get_payload_filter()
        pf.append('scSerialNumber', self.ssn)
        vfname = (self.vfname if self.vfname.endswith('/')
                  else self.vfname + '/')
        pf.append('volumeFolderPath', vfname)
        r = self.client.post('StorageCenter/ScVolume/GetList',
                             pf.payload)
        if self._check_result(r):
            retval = self._get_json(r)
            if isinstance(retval, list):
                result = retval
            elif retval:
                result.append(retval)
        return result

    def create_volume(self, name, size, storage_profile=None):
        """Creates a new volume on the Storage Center.

        It will create it in a folder called self.vfname.  If self.vfname
        does not exist it will create it.  If it cannot create it
        the volume will be created in the root.

        :param name: Name of the volume to be created on the Dell SC backend.
                     This is the cinder volume ID.
        :param size: The size of the volume to be created in GB.
        :param storage_profile: Optional storage profile to set for the volume.
        :returns: Dell Volume object or None.
        """
        LOG.debug('Create Volume %(name)s %(ssn)s %(folder)s %(profile)s',
                  {'name': name,
                   'ssn': self.ssn,
                   'folder': self.vfname,
                   'profile': storage_profile,
                   })

        # Find our folder
        folder = self._find_volume_folder(True)

        # If we actually have a place to put our volume create it
        if folder is None:
            LOG.warning('Unable to create folder %s',
                        self.vfname)

        # See if we need a storage profile
        profile = self._find_storage_profile(storage_profile)
        if storage_profile and profile is None:
            raise Exception('Storage Profile %s not found.' % storage_profile)

        # Init our return.
        scvolume = None

        # Create the volume
        payload = {}
        payload['Name'] = name
        payload['Notes'] = self.notes
        payload['Size'] = '%d GB' % size
        payload['StorageCenter'] = self.ssn
        if folder:
            payload['VolumeFolder'] = self._get_id(folder)
        if profile:
            payload['StorageProfile'] = self._get_id(profile)
        r = self.client.post('StorageCenter/ScVolume',
                             payload)
        if self._check_result(r):
            scvolume = self._get_json(r)
            if scvolume:
                LOG.info('Created volume %(instanceId)s: %(name)s',
                         {'instanceId': scvolume['instanceId'],
                          'name': scvolume['name']})
            else:
                LOG.error('ScVolume returned success with empty payload. '
                          'Attempting to locate volume.')
                # In theory it is there since success was returned.
                # Try one last time to find it before returning.
                scvolume = self.find_volume(name)
        else:
            raise Exception('ScVolume creation failed.')

        return scvolume

    def _get_volume_list(self, name, deviceid, filterbyvfname=True):
        """Return the specified list of volumes.

        :param name: Volume name.
        :param deviceid: Volume device ID on the SC backend.
        :param filterbyvfname:  If set to true then this filters by the preset
                                folder name.
        :return: Returns the scvolume list or None.
        """
        result = None
        # We need a name or a device ID to find a volume.
        if name or deviceid:
            pf = self._get_payload_filter()
            pf.append('scSerialNumber', self.ssn)
            if name is not None:
                pf.append('Name', name)
            if deviceid is not None:
                pf.append('DeviceId', deviceid)
            # set folderPath
            if filterbyvfname:
                vfname = (self.vfname if self.vfname.endswith('/')
                          else self.vfname + '/')
                pf.append('volumeFolderPath', vfname)
            r = self.client.post('StorageCenter/ScVolume/GetList',
                                 pf.payload)
            if self._check_result(r):
                result = self._get_json(r)
        # We return None if there was an error and a list if the command
        # succeeded. It might be an empty list.
        return result

    def find_volume(self, name):
        """Search self.ssn for volume of name.

        This searches the folder self.vfname (specified in the cinder.conf)
        for the volume first.  If not found it searches the entire array for
        the volume.

        :param name: Name of the volume to search for.  This is the cinder
                     volume ID.
        :returns: Dell Volume object or None if not found.
        :raises VolumeBackendAPIException: If multiple copies are found.
        """
        LOG.debug('Searching %(sn)s for %(name)s',
                  {'sn': self.ssn,
                   'name': name})

        # Cannot find a volume without the name
        if name is None:
            return None

        # Look for our volume in our folder.
        vollist = self._get_volume_list(name,
                                        None,
                                        True)
        # If an empty list was returned they probably moved the volumes or
        # changed the folder name so try again without the folder.
        if not vollist:
            LOG.debug('Cannot find volume %(n)s in %(v)s. Searching SC.',
                      {'n': name,
                       'v': self.vfname})
            vollist = self._get_volume_list(name,
                                            None,
                                            False)

        # If multiple volumes of the same name are found we need to error.
        if len(vollist) > 1:
            # blow up
            raise Exception('Multiple copies of volume %s found.' % name)

        # We made it and should have a valid volume.
        return None if not vollist else vollist[0]

    def delete_volume(self, name):
        """Deletes the volume from the SC backend array.

        If the volume cannot be found we claim success.

        :param name: Name of the volume to search for.  This is the cinder
                     volume ID.
        :returns: Boolean indicating success or failure.
        """
        if not isinstance(name, unicode):
            name = u"%s" % name
        vol = self.find_volume(name)
        if vol is not None:
            r = self.client.delete('StorageCenter/ScVolume/%s'
                                   % self._get_id(vol))
            if not self._check_result(r):
                raise Exception(
                    'Error deleting volume '
                    '%(ssn)s: %(volume)s: %(code)d %(reason)s' %
                    {'ssn': self.ssn,
                     'volume': name,
                     'code': r.status_code,
                     'reason': r.reason})
            # json return should be true or false
            return self._get_json(r)
        LOG.warning('delete_volume: unable to find volume %s',
                    name)
        # If we can't find the volume then it is effectively gone.
        return True

    def _find_server_folder(self, create=False):
        """Looks for the server folder on the Dell Storage Center.

         This is the folder where a server objects for mapping volumes will be
         created.  Server folder is specified in cinder.conf.  See __init.

        :param create: If True will create the folder if not found.
        :return: Folder object.
        """
        folder = self._find_folder('StorageCenter/ScServerFolder/GetList',
                                   self.sfname)
        if folder is None and create is True:
            folder = self._create_folder_path('StorageCenter/ScServerFolder',
                                              self.sfname)
        return folder

    def _add_hba(self, scserver, wwnoriscsiname, isfc=False):
        """This adds a server HBA to the Dell server object.

        The HBA is taken from the connector provided in initialize_connection.
        The Dell server object is largely a container object for the list of
        HBAs associated with a single server (or vm or cluster) for the
        purposes of mapping volumes.

        :param scserver: Dell server object.
        :param wwnoriscsiname: The WWN or IQN to add to this server.
        :param isfc: Boolean indicating whether this is an FC HBA or not.
        :returns: Boolean indicating success or failure.
        """
        payload = {}
        if isfc is True:
            payload['HbaPortType'] = 'FibreChannel'
        else:
            payload['HbaPortType'] = 'Iscsi'
        payload['WwnOrIscsiName'] = wwnoriscsiname
        payload['AllowManual'] = True
        r = self.client.post('StorageCenter/ScPhysicalServer/%s/AddHba'
                             % self._get_id(scserver),
                             payload)
        if not self._check_result(r):
            LOG.error('AddHba error: '
                      '%(wwn)s to %(srvname)s : %(code)d %(reason)s',
                      {'wwn': wwnoriscsiname,
                       'srvname': scserver['name'],
                       'code': r.status_code,
                       'reason': r.reason})
            return False
        return True

    def _find_serveros(self, osname='Red Hat Linux 6.x'):
        """Returns the serveros instance id of the specified osname.

        Required to create a Dell server object.

        We do not know that we are Red Hat Linux 6.x but that works
        best for Red Hat and Ubuntu.  So we use that.

        :param osname: The name of the OS to look for.
        :returns: InstanceId of the ScServerOperatingSystem object.
        """
        pf = self._get_payload_filter()
        pf.append('scSerialNumber', self.ssn)
        r = self.client.post('StorageCenter/ScServerOperatingSystem/GetList',
                             pf.payload)
        if self._check_result(r):
            oslist = self._get_json(r)
            for srvos in oslist:
                name = srvos.get('name', 'nope')
                if name.lower() == osname.lower():
                    # Found it return the id
                    return self._get_id(srvos)

        LOG.warning('ScServerOperatingSystem GetList return: '
                    '%(code)d %(reason)s',
                    {'code': r.status_code,
                     'reason': r.reason})
        return None

    def create_server_multiple_hbas(self, name, wwns):
        """Creates a server with multiple WWNS associated with it.

        Same as create_server except it can take a list of HBAs.

        :param name: The server's name.
        :param wwns: A list of FC WWNs or iSCSI IQNs associated with this
                     server.
        :returns: Dell server object.
        """
        scserver = None
        # Our instance names
        for wwn in wwns:
            if scserver is None:
                # Use the fist wwn to create the server.
                scserver = self.create_server(name,
                                              wwn,
                                              True)
            else:
                # Add the wwn to our server
                self._add_hba(scserver,
                              wwn,
                              True)
        return scserver

    def create_server(self, name, wwnoriscsiname, isfc=False):
        """Creates a Dell server object on the the Storage Center.

        Adds the first HBA identified by wwnoriscsiname to it.

        :param name: The server's name.
        :param wwnoriscsiname: A list of FC WWNs or iSCSI IQNs associated with
                               this Dell server object.
        :param isfc: Boolean indicating whether this is an FC HBA or not.
        :returns: Dell server object.
        """
        scserver = None
        payload = {}
        payload['Name'] = name or 'Server_%s' % wwnoriscsiname
        payload['StorageCenter'] = self.ssn
        payload['Notes'] = self.notes
        # We pick Red Hat Linux 6.x because it supports multipath and
        # will attach luns to paths as they are found.
        scserveros = self._find_serveros('Red Hat Linux 6.x')
        if scserveros is not None:
            payload['OperatingSystem'] = scserveros

        # Find our folder or make it
        folder = self._find_server_folder(True)

        # At this point it doesn't matter if the folder was created or not.
        # We just attempt to create the server.  Let it be in the root if
        # the folder creation fails.
        if folder is not None:
            payload['ServerFolder'] = self._get_id(folder)

        # create our server
        r = self.client.post('StorageCenter/ScPhysicalServer',
                             payload)
        if self._check_result(r):
            # Server was created
            scserver = self._first_result(r)

            # Add hba to our server
            if scserver is not None:
                if not self._add_hba(scserver,
                                     wwnoriscsiname,
                                     isfc):
                    LOG.error('Error adding HBA to server')
                    # Can't have a server without an HBA
                    self._delete_server(scserver)
                    scserver = None
        # Success or failure is determined by the caller
        return scserver

    def find_server(self, wwnoriscsiname):
        """Hunts for a server on the Dell backend by instance_name.

        The instance_name is the same as the server's HBA.  This is the  IQN or
        WWN listed in the connector.  If found, the server the HBA is attached
        to, if any, is returned.

        :param instance_name: instance_name is a FC WWN or iSCSI IQN from
                              the connector.  In cinder a server is identified
                              by its HBA.
        :returns: Dell server object or None.
        """
        scserver = None
        # We search for our server by first finding our HBA
        hba = self._find_serverhba(wwnoriscsiname)
        # Once created hbas stay in the system.  So it isn't enough
        # that we found one it actually has to be attached to a
        # server.
        if hba is not None and hba.get('server') is not None:
            pf = self._get_payload_filter()
            pf.append('scSerialNumber', self.ssn)
            pf.append('instanceId', self._get_id(hba['server']))
            r = self.client.post('StorageCenter/ScServer/GetList',
                                 pf.payload)
            if self._check_result(r):
                scserver = self._first_result(r)
        if scserver is None:
            LOG.debug('Server (%s) not found.', wwnoriscsiname)
        return scserver

    def _find_serverhba(self, instance_name):
        """Hunts for a server HBA on the Dell backend by instance_name.

        Instance_name is the same as the IQN or WWN specified in the
        connector.

        :param instance_name: Instance_name is a FC WWN or iSCSI IQN from
                              the connector.
        :returns: Dell server HBA object.
        """
        scserverhba = None
        # We search for our server by first finding our HBA
        pf = self._get_payload_filter()
        pf.append('scSerialNumber', self.ssn)
        pf.append('instanceName', instance_name)
        r = self.client.post('StorageCenter/ScServerHba/GetList',
                             pf.payload)
        if self._check_result(r):
            scserverhba = self._first_result(r)
        return scserverhba

    def _find_domains(self, cportid):
        """Find the list of Dell domain objects associated with the cportid.

        :param cportid: The Instance ID of the Dell controller port.
        :returns: List of fault domains associated with this controller port.
        """
        r = self.client.get('StorageCenter/ScControllerPort/%s/FaultDomainList'
                            % cportid)
        if self._check_result(r):
            domains = self._get_json(r)
            return domains
        else:
            LOG.debug('FaultDomainList error: %(code)d %(reason)s',
                      {'code': r.status_code,
                       'reason': r.reason})
            LOG.error('Error getting FaultDomainList')
        return None

    def _find_fc_initiators(self, scserver):
        """Returns a list of FC WWNs associated with the specified Dell server.

        :param scserver: The Dell backend server object.
        :returns: A list of FC WWNs associated with this server.
        """
        initiators = []
        r = self.client.get('StorageCenter/ScServer/%s/HbaList'
                            % self._get_id(scserver))
        if self._check_result(r):
            hbas = self._get_json(r)
            for hba in hbas:
                wwn = hba.get('instanceName')
                if (hba.get('portType') == 'FibreChannel' and
                        wwn is not None):
                    initiators.append(wwn)
        else:
            LOG.debug('HbaList error: %(code)d %(reason)s',
                      {'code': r.status_code,
                       'reason': r.reason})
            LOG.error('Unable to find FC initiators')
        LOG.debug(initiators)
        return initiators

    def _find_mappings(self, scvolume):
        """Find the Dell volume object mappings.

        :param scvolume: Dell volume object.
        :returns: A list of Dell mappings objects.
        """
        mappings = []
        if scvolume.get('active', False):
            r = self.client.get('StorageCenter/ScVolume/%s/MappingList'
                                % self._get_id(scvolume))
            if self._get_result(r):
                mappings = self._get_json(r)
            else:
                LOG.debug('MappingList error: %(code)d %(reason)s',
                          {'code': r.status_code,
                           'reason': r.reason})
                LOG.error('Unable to find volume mappings: %s',
                          scvolume.get('name'))
        else:
            LOG.error('_find_mappings: volume is not active')
        LOG.debug(mappings)
        return mappings

    def find_mapping_profiles(self, scvolume):
        """Find the Dell volume object mapping profiles.

        :param scvolume: Dell volume object.
        :returns: A list of Dell mapping profile objects.
        """
        mapping_profiles = []
        r = self.client.get('StorageCenter/ScVolume/%s/MappingProfileList'
                            % self._get_id(scvolume))
        if self._check_result(r):
            mapping_profiles = self._get_json(r)
        return mapping_profiles

    def _find_controller_port(self, cportid):
        """Finds the SC controller port object for the specified cportid.

        :param cportid: The instanceID of the Dell backend controller port.
        :returns: The controller port object.
        """
        controllerport = None
        r = self.client.get('StorageCenter/ScControllerPort/%s'
                            % cportid)
        if self._check_result(r):
            controllerport = self._first_result(r)
        else:
            LOG.debug('ScControllerPort error: %(code)d %(reason)s',
                      {'code': r.status_code,
                       'reason': r.reason})
            LOG.error('Unable to find controller port: %s',
                      cportid)
        LOG.debug(controllerport)
        return controllerport

    def find_wwns(self, scvolume, scserver):
        """Finds the lun and wwns of the mapped volume.

        :param scvolume: Storage Center volume object.
        :param scserver: Storage Center server opbject.
        :returns: Lun, wwns, initiator target map
        """
        lun = None  # our lun.  We return the first lun.
        wwns = []  # list of targets
        itmap = {}  # dict of initiators and the associated targets

        # Make sure we know our server's initiators.  Only return
        # mappings that contain HBA for this server.
        initiators = self._find_fc_initiators(scserver)
        # Get our volume mappings
        mappings = self._find_mappings(scvolume)
        if len(mappings) > 0:
            # We check each of our mappings.  We want to return
            # the mapping we have been configured to use.
            for mapping in mappings:
                # Find the controller port for this mapping
                cport = mapping.get('controllerPort')
                controllerport = self._find_controller_port(
                    self._get_id(cport))
                if controllerport is not None:
                    # This changed case at one point or another.
                    # Look for both keys.
                    wwn = controllerport.get('wwn',
                                             controllerport.get('WWN'))
                    if wwn:
                        serverhba = mapping.get('serverHba')
                        if serverhba:
                            hbaname = serverhba.get('instanceName')
                            if hbaname in initiators:
                                if itmap.get(hbaname) is None:
                                    itmap[hbaname] = []
                                itmap[hbaname].append(wwn)
                                wwns.append(wwn)

                                mappinglun = mapping.get('lun')
                                if lun is None:
                                    lun = mappinglun
                                elif lun != mappinglun:
                                    LOG.warning('Inconsistent Luns.')
                            else:
                                LOG.debug('%s not found in initiator list',
                                          hbaname)
                        else:
                            LOG.debug('serverhba is None.')
                    else:
                        LOG.debug('Unable to find port wwn.')
                else:
                    LOG.debug('controllerport is None.')
        else:
            LOG.error('Volume appears unmapped')
        LOG.debug(lun)
        LOG.debug(wwns)
        LOG.debug(itmap)
        # TODO(tom_swanson): if we have nothing to return raise an exception
        # here.  We can't do anything with an unmapped volume.  We shouldn't
        # pretend we succeeded.
        return lun, wwns, itmap

    def _find_active_controller(self, scvolume):
        """Finds the controller on which the Dell volume is active.

        There can be more than one Dell backend controller per Storage center
        but a given volume can only be active on one of them at a time.

        :param scvolume: Dell backend volume object.
        :returns: Active controller ID.
        """
        actvctrl = None
        # TODO(Swanson): We have a function that gets this.  Call that.
        r = self.client.get('StorageCenter/ScVolume/%s/VolumeConfiguration'
                            % self._get_id(scvolume))
        if self._check_result(r):
            volconfig = self._first_result(r)
            controller = volconfig.get('controller')
            actvctrl = self._get_id(controller)
        else:
            LOG.debug('VolumeConfiguration error: %(code)d %(reason)s',
                      {'code': r.status_code,
                       'reason': r.reason})
            LOG.error('Unable to retrieve VolumeConfiguration: %s',
                      self._get_id(scvolume))
        LOG.debug('activecontroller %s', actvctrl)
        return actvctrl

    def _get_controller_id(self, mapping):
        # The mapping lists the associated controller.
        return self._get_id(mapping.get('controller'))

    def _get_domains(self, mapping):
        # Return a list of domains associated with this controller port.
        return self._find_domains(self._get_id(mapping.get('controllerPort')))

    def _get_iqn(self, mapping):
        # Get our iqn from the controller port listed in our our mapping.
        iqn = None
        cportid = self._get_id(mapping.get('controllerPort'))
        controllerport = self._find_controller_port(cportid)
        LOG.debug('controllerport: %s', controllerport)
        if controllerport:
            iqn = controllerport.get('iscsiName')
        return iqn

    def _is_virtualport_mode(self):
        isvpmode = False
        r = self.client.get('StorageCenter/ScConfiguration/%s' % self.ssn)
        if self._check_result(r):
            scconfig = self._get_json(r)
            if scconfig:
                isvpmode = True if (scconfig['iscsiTransportMode'] ==
                                    'VirtualPort') else False
        return isvpmode

    def _find_controller_port_iscsi_config(self, cportid):
        """Finds the SC controller port object for the specified cportid.

        :param cportid: The instanceID of the Dell backend controller port.
        :returns: The controller port object.
        """
        controllerport = None
        r = self.client.get('StorageCenter/'
                            'ScControllerPortIscsiConfiguration/%s'
                            % cportid)
        if self._check_result(r):
            controllerport = self._first_result(r)
        else:
            LOG.debug('ScControllerPortIscsiConfiguration error: '
                      '%(code)d %(reason)s',
                      {'code': r.status_code,
                       'reason': r.reason})
            LOG.error('Unable to find controller '
                      'port iscsi configuration: %s',
                      cportid)
        return controllerport

    def find_iscsi_properties(self, scvolume, ip=None, port=None):
        """Finds target information for a given Dell scvolume object mapping.

        The data coming back is both the preferred path and all the paths.

        :param scvolume: The dell sc volume object.
        :param ip: The preferred target portal ip.
        :param port: The preferred target portal port.
        :returns: iSCSI property dictionary.
        :raises: VolumeBackendAPIException
        """
        LOG.debug('enter find_iscsi_properties')
        LOG.debug('scvolume: %s', scvolume)
        # Our mutable process object.
        pdata = {'active': -1,
                 'up': -1,
                 'access_mode': 'rw',
                 'ip': ip,
                 'port': port}
        # Our output lists.
        portals = []
        luns = []
        iqns = []

        # Process just looks for the best port to return.
        def process(lun, iqn, address, port, readonly, status, active):
            """Process this mapping information.

            :param lun: SCSI Lun.
            :param iqn: iSCSI IQN address.
            :param address: IP address.
            :param port: IP Port number
            :param readonly: Boolean indicating mapping is readonly.
            :param status: String indicating mapping status.  (Up is what we
                           are looking for.)
            :param active: Boolean indicating whether this is on the active
                           controller or not.
            :return: Nothing
            """
            portals.append(address + ':' +
                           six.text_type(port))
            iqns.append(iqn)
            luns.append(lun)

            # We've all the information.  We need to find
            # the best single portal to return.  So check
            # this one if it is on the right IP, port and
            # if the access and status are correct.
            if ((pdata['ip'] is None or pdata['ip'] == address) and
                    (pdata['port'] is None or pdata['port'] == port)):

                # We need to point to the best link.
                # So state active and status up is preferred
                # but we don't actually need the state to be
                # up at this point.
                if pdata['up'] == -1:
                    pdata['access_mode'] = 'rw' if readonly is False else 'ro'
                    if active:
                        pdata['active'] = len(iqns) - 1
                        if status == 'Up':
                            pdata['up'] = pdata['active']

        # Start by getting our mappings.
        mappings = self._find_mappings(scvolume)

        # We should have mappings at the time of this call but do check.
        if len(mappings) > 0:
            # In multipath (per Liberty) we will return all paths.  But
            # if multipath is not set (ip and port are None) then we need
            # to return a mapping from the controller on which the volume
            # is active.  So find that controller.
            actvctrl = self._find_active_controller(scvolume)
            # Two different methods are used to find our luns and portals
            # depending on whether we are in virtual or legacy port mode.
            isvpmode = self._is_virtualport_mode()
            # Trundle through our mappings.
            for mapping in mappings:
                # The lun, ro mode and status are in the mapping.
                LOG.debug('mapping: %s', mapping)
                lun = mapping.get('lun')
                ro = mapping.get('readOnly', False)
                status = mapping.get('status')
                # Get our IQN from our mapping.
                iqn = self._get_iqn(mapping)
                # Check if our controller ID matches our active controller ID.
                isactive = True if (self._get_controller_id(mapping) ==
                                    actvctrl) else False
                # If we have an IQN and are in virtual port mode.
                if isvpmode and iqn:
                    domains = self._get_domains(mapping)
                    if domains:
                        for dom in domains:
                            LOG.debug('domain: %s', dom)
                            ipaddress = dom.get('targetIpv4Address',
                                                dom.get('wellKnownIpAddress'))
                            portnumber = dom.get('portNumber')
                            # We have all our information. Process this portal.
                            process(lun, iqn, ipaddress, portnumber,
                                    ro, status, isactive)
                # Else we are in legacy mode.
                elif iqn:
                    # Need to get individual ports
                    cportid = self._get_id(mapping.get('controllerPort'))
                    # Legacy mode stuff is in the ISCSI configuration object.
                    cpconfig = self._find_controller_port_iscsi_config(cportid)
                    # This should really never fail. Things happen so if it
                    # does just keep moving. Return what we can.
                    if cpconfig:
                        ipaddress = cpconfig.get('ipAddress')
                        portnumber = cpconfig.get('portNumber')
                        # We have all our information.  Process this portal.
                        process(lun, iqn, ipaddress, portnumber,
                                ro, status, isactive)

        # We've gone through all our mappings.
        # Make sure we found something to return.
        if len(luns) == 0:
            # Since we just mapped this and can't find that mapping the world
            # is wrong so we raise exception.
            raise Exception('Unable to find iSCSI mappings.')

        # Make sure we point to the best portal we can.  This means it is
        # on the active controller and, preferably, up.  If it isn't return
        # what we have.
        if pdata['up'] != -1:
            # We found a connection that is already up.  Return that.
            pdata['active'] = pdata['up']
        elif pdata['active'] == -1:
            # This shouldn't be able to happen.  Maybe a controller went
            # down in the middle of this so just return the first one and
            # hope the ports are up by the time the connection is attempted.
            LOG.debug('Volume is not yet active on any controller.')
            pdata['active'] = 0

        data = {'target_discovered': False,
                'target_iqn': iqns[pdata['active']],
                'target_iqns': iqns,
                'target_portal': portals[pdata['active']],
                'target_portals': portals,
                'target_lun': luns[pdata['active']],
                'target_luns': luns,
                'access_mode': pdata['access_mode']
                }
        LOG.debug('find_iscsi_properties return: %s',
                  data)

        return data

    def get_iscsi_ports(self):
        """Gets the array's iSCSI ports.

        Looks for the fault domain ports in virtual port mode or the front-end
        primary ports in legacy mode.
        :returns: List of (ip, port) tuples.
        """
        result = []
        pf = self._get_payload_filter()                                                                                        
        pf.append('scSerialNumber', self.ssn)
        pf.append('TransportType', 'Iscsi')
        r = self.client.post('StorageCenter/ScFaultDomain/GetList',
                             pf.payload)
        if self._check_result(r):
            fault_domains = r.json()
            for fault_domain in fault_domains:
                if fault_domain['targetIpv4Address'] != '0.0.0.0':
                    result.append(
                        (fault_domain['targetIpv4Address'],
                         fault_domain['portNumber']))

        if len(result) == 0:
            # Must be running legacy mode, look for all front end primary
            # ports.
            pf = self._get_payload_filter()
            pf.append('scSerialNumber', self.ssn)
            r = self.client.post(
                'StorageCenter/ScControllerPortIscsiConfiguration/GetList',
                pf.payload)
            if self._check_result(r):
                ports = r.json()
                for port in ports:
                    result.append(
                        (port['ipAddress'],
                         port['portNumber']))
        return result

    def map_volume(self, scvolume, scserver):
        """Maps the Dell backend volume object to the Dell server object.

        The check for the Dell server object existence is elsewhere;  does not
        create the Dell server object.

        :param scvolume: Storage Center volume object.
        :param scserver: Storage Center server opbject.
        :returns: SC mapping profile or None
        """
        # Make sure we have what we think we have
        serverid = self._get_id(scserver)
        volumeid = self._get_id(scvolume)
        if serverid is not None and volumeid is not None:
            # If we have a mapping to our server return it here.
            mprofiles = self.find_mapping_profiles(scvolume)
            for mprofile in mprofiles:
                if self._get_id(mprofile.get('server')) == serverid:
                    return mprofile
            # No?  Then map it up.
            payload = {}
            payload['server'] = serverid
            advanced = {}
            advanced['MapToDownServerHbas'] = True
            # NOTE: for now we are just doing single pathing
            advanced['MaximumPathCount'] = 1
            advanced['BootVolume'] = False
            advanced['NoPreferredUseNextAvailable'] = True
            advanced['UseNextAvailable'] = True
            payload['Advanced'] = advanced
            r = self.client.post('StorageCenter/ScVolume/%s/MapToServer'
                                 % volumeid,
                                 payload)
            if r.status_code == 200:
                # We just return our mapping
                return self._first_result(r)
            # Should not be here.
            LOG.debug('MapToServer error: %(code)d %(reason)s',
                      {'code': r.status_code,
                       'reason': r.reason})
        # Error out
        LOG.error('Unable to map %(vol)s to %(srv)s',
                  {'vol': scvolume['name'],
                   'srv': scserver['name']})
        return None

    def unmap_volume(self, scvolume, scserver):
        """Unmaps the Dell volume object from the Dell server object.

        Deletes all mappings to a Dell server object, not just the ones on
        the path defined in cinder.conf.

        :param scvolume: Storage Center volume object.
        :param scserver: Storage Center server opbject.
        :returns: True or False.
        """
        rtn = True
        serverid = self._get_id(scserver)
        volumeid = self._get_id(scvolume)
        if serverid is not None and volumeid is not None:
            profiles = self.find_mapping_profiles(scvolume)
            for profile in profiles:
                prosrv = profile.get('server')
                if prosrv is not None and self._get_id(prosrv) == serverid:
                    r = self.client.delete('StorageCenter/ScMappingProfile/%s'
                                           % self._get_id(profile))
                    if not self._check_result(r):
                        LOG.debug('ScMappingProfile error: '
                                  '%(code)d %(reason)s',
                                  {'code': r.status_code,
                                   'reason': r.reason})
                        LOG.error('Unable to unmap Volume %s',
                                  volumeid)
                        # 1 failed unmap is as good as 100.
                        # Fail it and leave
                        rtn = False
                        break
                    LOG.debug('Volume %(vol)s unmapped from %(srv)s',
                              {'vol': volumeid,
                               'srv': serverid})
        return rtn

    def expand_volume(self, scvolume, newsize):
        """Expands scvolume to newsize GBs.

        :param scvolume: Dell volume object to be expanded.
        :param newsize: The new size of the volume object.
        :returns: The updated Dell volume object on success or None on failure.
        """
        payload = {}
        payload['NewSize'] = '%d GB' % newsize
        r = self.client.post('StorageCenter/ScVolume/%s/ExpandToSize'
                             % self._get_id(scvolume),
                             payload)
        vol = None
        if self._check_result(r):
            vol = self._get_json(r)
        else:
            LOG.error('Error expanding volume '
                      '%(name)s: %(code)d %(reason)s',
                      {'name': scvolume['name'],
                       'code': r.status_code,
                       'reason': r.reason})
        if vol is not None:
            LOG.debug('Volume expanded: %(name)s %(size)s',
                      {'name': vol['name'],
                       'size': vol['configuredSize']})
        return vol

    def update_storage_profile(self, scvolume, storage_profile):
        """Update a volume's Storage Profile.

        Changes the volume setting to use a different Storage Profile. If
        storage_profile is None, will reset to the default profile for the
        cinder user account.

        :param scvolume: The Storage Center volume to be updated.
        :param storage_profile: The requested Storage Profile name.
        :returns: True if successful, False otherwise.
        """
        prefs = self._get_user_preferences()
        if not prefs:
            return False

        if not prefs.get('allowStorageProfileSelection'):
            LOG.error('User does not have permission to change '
                      'Storage Profile selection.')
            return False

        profile = self._find_storage_profile(storage_profile)
        if storage_profile:
            if not profile:
                LOG.error('Storage Profile %s was not found.',
                          storage_profile)
                return False
        else:
            # Going from specific profile to the user default
            profile = prefs.get('storageProfile')
            if not profile:
                LOG.error('Default Storage Profile was not found.')
                return False

        LOG.info('Switching volume %(vol)s to profile %(prof)s.',
                 {'vol': scvolume['name'],
                  'prof': profile.get('name')})
        payload = {}
        payload['StorageProfile'] = self._get_id(profile)
        r = self.client.post('StorageCenter/ScVolumeConfiguration'
                             '/%s/Modify'
                             % self._get_id(scvolume),
                             payload)
        if not self._check_result(r):
            LOG.error('Error changing Storage Profile for volume '
                      '%(original)s to %(name)s: %(code)d %(reason)s '
                      '%(text)s',
                      {'original': scvolume['name'],
                       'name': storage_profile,
                       'code': r.status_code,
                       'reason': r.reason,
                       'text': r.text})
            return False
        return True

    def _get_user_preferences(self):
        """Gets the preferences and defaults for this user.

        There are a set of preferences and defaults for each user on the
        Storage Center. This retrieves all settings for the current account
        used by Cinder.
        """
        r = self.client.get('StorageCenter/StorageCenter/%s/UserPreferences' %
                            self.ssn)
        if not self._check_result(r):
            LOG.error('Error getting user preferences: '
                      '%(code)d %(reason)s %(text)s',
                      {'code': r.status_code,
                       'reason': r.reason,
                       'text': r.text})
            return {}
        return self._get_json(r)

    def _delete_server(self, scserver):
        """Deletes scserver from the backend.

        Just give it a shot.  If it fails it doesn't matter to cinder.  This
        is generally used when a create_server call fails in the middle of
        creation.  Cinder knows nothing of the servers objects on Dell backends
        so success or failure is purely an internal thing.

        Note that we do not delete a server object in normal operation.

        :param scserver: Dell server object to delete.
        :returns: Nothing.  Only logs messages.
        """
        if scserver.get('deleteAllowed') is True:
            r = self.client.delete('StorageCenter/ScServer/%s'
                                   % self._get_id(scserver))
            LOG.debug('ScServer %(id)s delete return: %(code)d %(reason)s',
                      {'id': self._get_id(scserver),
                       'code': r.status_code,
                       'reason': r.reason})
        else:
            LOG.debug('_delete_server: deleteAllowed is False.')

    def _get_volume_configuration(self, scvolume):
        """Get the ScVolumeConfiguration object.

        :param scvolume: The Dell SC volume object.
        :return: The SCVolumeConfiguration object or None.
        """
        r = self.client.get('StorageCenter/ScVolume/%s/VolumeConfiguration' %
                            self._get_id(scvolume))
        if self._check_result(r):
            LOG.debug('get_volume_configuration %s', r)
            return self._first_result(r)
        return None