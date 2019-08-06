#!/usr/bin/env python

import setuptools

from chord_federation_async import __version__

with open("README.md", "r") as rf:
    long_description = rf.read()

setuptools.setup(
    name="chord_federation_async",
    version=__version__,

    python_requires=">=3.6",
    install_requires=["tornado"],

    author="David Lougheed",
    author_email="david.lougheed@mail.mcgill.ca",

    description="Search federation service for the CHORD project.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    packages=["chord_federation_async"],
    include_package_data=True,

    url="TODO",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ]
)
