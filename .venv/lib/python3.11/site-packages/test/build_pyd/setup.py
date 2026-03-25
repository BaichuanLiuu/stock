# -*- coding: utf-8 -*-
"""
Created on 2021-12-31 09:32:16
---------
@summary:  
---------
@author: yangyx01
"""
# setup.py
from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize("rtqc.py")
)
