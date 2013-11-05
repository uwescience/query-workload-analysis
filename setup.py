#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='SDSS Query Analysis',
      version='0.0.1',
      description='A collection of scripts to analyze the SDSS queries.',
      author='Dan Halperin, Dominik Moritz',
      author_email='',
      packages=find_packages(exclude=['tests']),
      install_requires=[
          'dataset',
          'pymssql',
          'docopt'
      ],
      entry_points={
            'console_scripts': [
                  'sdss_tools = analyzesdss.main:main'
            ],
      },
     )