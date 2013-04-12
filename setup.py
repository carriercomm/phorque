#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="phorque",
    version="0.1",
    scripts=["bin/phorque.py"],
    packages=find_packages(),
    author="Paul Marshall",
    author_email="paul.marshall@colorado.edu",
    license=open("LICENSE.txt").read(),
)
