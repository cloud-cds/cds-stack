from distutils.core import setup
from setuptools import setup, find_packages

setup(
    name='etl',
    version='1.0',
    package_dir={'etl': ''},
    packages=["etl." + package for package in find_packages()],
    package_data={'': ['*.csv', '*.conf', '*.yaml']},
)
