# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='maxwell-faker',
    version='0.1.0',
    description='Maxwell faker for systems and load testing',
    url='https://github.com/movio/maxwell-faker',
    author='Nicolas Maquet and Nan Wu',
    author_email='nicolas@movio.co, nan@movio.co',
    license='MIT',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: Database',
        'Topic :: Software Development :: Testing'
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='maxwell faker fake data generator json kafka mysql',
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'PyYAML',
        'kafka-python'
    ],
    entry_points={
        'console_scripts': [
            'maxwell-faker=maxwell_faker:daemon_main',
            'maxwell-faker-bootstrap=maxwell_faker:bootstrap_main',
        ],
    },
)
