# coding=utf-8

from setuptools import setup


setup(
    name='python-mysql-simple-wrapper',
    version='1.2.9',
    url='https://github.com/secfree/python-mysql-simple-wrapper',
    author='secfree',
    description='A simple wrapper for operation on mysql.',
    long_description=__doc__,
    py_modules=['mysql_simple_wrapper'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=[
        'mysql-connector'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ]
)
