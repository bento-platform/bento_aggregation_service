#!/usr/bin/env python

import configparser
import os
import setuptools

with open("README.md", "r") as rf:
    long_description = rf.read()

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "bento_federation_service", "package.cfg"))

setuptools.setup(
    name=config["package"]["name"],
    version=config["package"]["version"],

    python_requires=">=3.6",
    install_requires=["bento_lib==3.0.1", "tornado>=6.1,<6.2"],

    author=config["package"]["authors"],
    author_email=config["package"]["author_emails"],

    description="Search federation service for the Bento platform.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    packages=setuptools.find_packages(),
    include_package_data=True,

    entry_points={
        "console_scripts": ["bento_federation_service=bento_federation_service.app:run"]
    },

    url="https://github.com/bento-platform/bento_federation_service",
    license="LGPLv3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Operating System :: OS Independent"
    ]
)
