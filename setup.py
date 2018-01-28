#!/usr/bin/env python

from setuptools import setup, find_packages
VERSION = '0.0.1'

setup(
    name='bicis',
    packages=find_packages(),
    version=VERSION,
    description='bicis',
    author='Pablo Zivic',
    author_email='elsonidoq@gmail.com',
    url='https://github.com/elsonidoq/bicis',
    download_url='https://github.com/elsonidoq/bicis/tarball/' + VERSION,
    zip_safe=False,
    install_requires=[
    ],
)
