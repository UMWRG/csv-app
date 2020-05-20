#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='hydra-csv',
    version='0.2',
    description="Hydra plug-in to import and export CSV files",
    packages=find_packages(),
    package_data={},
    include_package_data=True,
    entry_points='''
    [console_scripts]
    hydra-csv=hydra_csv.cli:start_cli
    ''',
)
