#!/usr/bin/env python

import setuptools

from chord_federation import __version__

with open("README.md", "r") as rf:
    long_description = rf.read()

setuptools.setup(
    name="chord_federation",
    version=__version__,

    python_requires=">=3.6",
    install_requires=["tornado"],

    author="David Lougheed",
    author_email="david.lougheed@mail.mcgill.ca",

    description="Search federation service for the CHORD project.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    packages=["chord_federation"],
    include_package_data=True,

    entry_points={
        "console_scripts": ["chord_federation=chord_federation.app:run"]
    },

    url="TODO",
    license="LGPLv3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ]
)
