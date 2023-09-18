#!/usr/bin/python

import setuptools

setuptools.setup(
    name='Coding-Challenge',
    version='1.0',
    install_requires=[
        'apache-beam[gcp]==2.48.0',
        'geopy==2.3.0',
    ],
    packages=setuptools.find_packages(exclude=['notebooks']),
    py_modules=['config'],
    include_package_data=True,
    description='Coding Challenge'
 )
