#!/usr/bin/env python3
import os

import setuptools

version = {}
with open("slice_db/version.py", "r") as f:
    exec(f.read(), version)

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    author="Rivet Health",
    author_email="ops@rivethealth.com",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    description="Translate between MLLP and HTTP",
    entry_points={
        "console_scripts": [
            "slicedb=slice_db.cli.main:main",
        ]
    },
    extras_require={
        "dev": ["black", "psutil", "pytest-env", "isort", "pytest", "snapshottest"]
    },
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        "asyncpg",
        "dataclasses_json==0.3.7",
        "jsonpath-ng",
        "jsonschema",
        "numpy",
        "pg-sql",
        "pyffx",
        "uvloop",
    ],
    name="slice-db",
    packages=setuptools.find_packages(),
    package_data={
        "slice_db.formats": ["*.json"],
        "slice_db.data": ["*.txt"],
    },
    project_urls={
        "Issues": "https://github.com/rivethealth/slice-db/issues",
    },
    python_requires=">3.7.0",
    url="https://github.com/rivethealth/slice-db",
    version=version["__version__"],
)
