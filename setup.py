#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-rockgympro",
    version="0.1.14",
    description="Singer.io tap for extracting RockGymPro data",
    author="Cinch",
    url="https://github.com/cinchio/tap-rockgympro",
    python_requires='>=3.6.0',
    py_modules=["tap_rockgympro"],
    install_requires=[
        "singer-python==5.12.1",
        "requests==2.26.0"
    ],
    entry_points="""
    [console_scripts]
    tap-rockgympro=tap_rockgympro:main
    """,
    packages=find_packages(include=['tap_rockgympro', 'tap_rockgympro.*']),
    package_data = {
        "tap_rockgympro": ["schemas/*.json"]
    },
    include_package_data=True,
)
