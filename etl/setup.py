from distutils.core import setup

setup(
    name='etl',
    version='1.0',
    package_dir={'etl': ''},
    packages=['etl'],
    package_data={'': ['*.csv', '*.conf']},
)
