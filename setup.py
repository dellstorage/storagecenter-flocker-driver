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
"""Installation for Dell Storage Center Plugin for Flocker."""

from setuptools import find_packages
from setuptools import setup

setup(
    name='dell_storagecenter_driver',
    version='1.0',
    description='Dell Storage Center Plugin for Flocker',
    license='Apache 2.0',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2.7',
    ],

    install_requires=['requests>=2.5.2'],

    keywords='backend, plugin, flocker, docker, python',
    packages=find_packages(exclude=['test*']),
    author='Sean McGinnis',
    author_email='sean_mcginnis@dell.com',
    url='https://github.com/dellstorage/storagecenter-flocker-driver',
    download_url='https://github.com/dellstorage/storagecenter-flocker-driver/tarball/1.0',
    data_files=[('/etc/flocker', ['example.sc_agent.yml'])]
)
