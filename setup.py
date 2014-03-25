#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

try:
    from setuptools import setup
    from setuptools import find_packages
except ImportError:
    from distutils.core import setup
import sys
import os

import anta
from anta.util import constants

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'requirements.txt')) as requirements_file, 
            open(os.path.join(here, 'README.md')) as readme_file, 
            open(os.path.join(here, 'LICENSE.CECILL-C')) as license_file:

    REQUIREMENTS = requirements_file.read().splitlines()
    README = readme_file.read()
    LICENSE = license_file.read()

    setup(
        name='ANTA2',
        version=constants.VERSION,
        author='Sciences Po médialab',
        author_email='medialab@sciences-po.fr',
        url='https://github.com/medialab/ANTA2',
        download_url='https://github.com/medialab/ANTA2',
        description='Actor Network Text Analyser v2',
        long_description=README,
        include_package_data=True,
        packages=find_packages('anta'),
        #packages=['biblib'],
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Environment :: Console',
            'Environment :: Web Environment',
            'Intended Audience :: End Users/Desktop',
            'Intended Audience :: Developers',
            'Operating System :: MacOS :: MacOS X',
            'Operating System :: Microsoft :: Windows',
            'Operating System :: POSIX',
            'Programming Language :: Python :: 2.7',
            'Topic :: Utilities'
        ],
        keywords='text analyser part-of-speech solr metrics tfidf locality genericity',
        license=LICENSE,
        install_requires=REQUIREMENTS,
        entry_points={
            'console_scripts': ['anta=anta:main']
        }
    )
