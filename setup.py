#!/usr/bin/env python
# Copyright (C) 2010 Chris Lewis
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors : Chris Lewis <cflewis@soe.ucsc.edu>

import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "cvsanaly",
    version = "2.4",
    author = "Chris Lewis",
    author_email = "cflewis@soe.ucsc.edu",
    description = ("A Python library for analyzing source control repositories"),
    license = "GPL version 2",
    keywords = "cvs svn git source sourcecontrol scm",
    url = "https://github.com/Lewisham/cvsanaly",
    packages=['pycvsanaly2', 'pycvsanaly2.extensions'],
    long_description=read('README.mdown'),
    scripts = ["cvsanaly2"],
    install_requires=['repositoryhandler>=0.3.6.1', 'guilty>=2.0'],
    dependency_links = ['https://github.com/SoftwareIntrospectionLab/repositoryhandler/tarball/master#egg=repositoryhandler-0.3.4',
    'https://github.com/SoftwareIntrospectionLab/guilty/tarball/master#egg=guilty-2.0'],
    classifiers = [
        "Development Status :: 4 - Beta",
        "Topic :: Software Development :: Version Control",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Environment :: Console"
    ],
)

