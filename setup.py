#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import dask_deploy

setup(name='dask-deploy',
      version=dask_deploy.__version__,
      description='Useful patterns and components for deploying dask',
      url='http://github.com/eriknw/dask-deploy/',
      maintainer='Erik Welch',
      maintainer_email='erik.n.welch@gmail.com',
      license='BSD',
      keywords='dask',
      packages=['dask_deploy'],
      package_data={'dask_deploy': ['tests/*.py']},
      long_description=(open('README.rst').read() if exists('README.rst') else ''),
      install_requires=list(open('requirements.txt').read().strip().split('\n')),
      zip_safe=False,
)
