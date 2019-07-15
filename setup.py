#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='fast_trader',
    version='0.1.0',
    packages=find_packages(),
    author='notmeor',
    author_email='kevin.inova@gmail.com',
    description='',
    include_package_data=True,
    package_data={'': ['config.yaml']},
    install_requires=[
        'pyzmq>=18.0.0',
        'sqlalchemy>=1.1.15',
        'inflection>=0.3.1',
        'PyYAML>=3.12',
        'pandas>=0.21.0']
)
