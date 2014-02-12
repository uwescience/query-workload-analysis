#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='Query Workload Analysis',
      version='0.0.1',
      description='A collection of scripts to analyze the query workloads.',
      author='Dan Halperin, Dominik Moritz',
      author_email='',
      packages=find_packages(exclude=['tests']),
      entry_points={
                  'console_scripts': [
                  'qwla = workloadanalysis.main:main'
                  ],
      },)
