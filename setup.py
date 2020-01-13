#!/usr/bin/env python

import setuptools

with open("README.md", "r") as rf:
    long_description = rf.read()

setuptools.setup(
    name="chord_federation",
    version="0.1.0",

    python_requires=">=3.6",
    install_requires=["chord_lib @ git+https://github.com/c3g/chord_lib", "tornado"],

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
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Operating System :: OS Independent"
    ]
)
