#! /usr/bin/env python
from __future__ import print_function

from setuptools import setup, Extension

USE_CYTHON = False

DISTNAME = 'resolver'
DESCRIPTION = 'A library for resolving disparate data sets.'

MAINTAINER = 'Carl Best'
MAINTAINER_EMAIL = 'carlcbest@gmail.com'
#URL = 'https://github.com/'
#DOWNLOAD_URL = 'https://github.com/'

VERSION = '0.1.0'

setup(
    name=DISTNAME,
    version=VERSION,
    description=DESCRIPTION,
    author=MAINTAINER,
    author_email=MAINTAINER_EMAIL,
    packages=['lsh'],
    ext_modules=extensions,
    install_requires=install_deps,
)