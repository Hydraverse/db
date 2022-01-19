"""Setup file for package.

See https://github.com/pypa/sampleproject/blob/main/setup.py
    for a full list of setup() options.
"""
import sys
import os

from setuptools import find_packages
from setuptools import setup

import hydb

# noinspection Assert
assert sys.version_info[0] == 3 and sys.version_info[1] >= 8, "This package requires Python 3.8 or newer"

setup(
    name="hydb",
    url="https://github.com/hydraverse/db",
    author="Halospace Foundation",
    author_email="contact@halospace.org",
    version=hydb.VERSION,
    description=hydb.__doc__,
    long_description=open(os.path.join(os.path.dirname(__file__), "README.md")).read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=[
        "halo-hypy",
        "pyyaml",
        "pydantic",
    ],
    extras_require={
        "client": [],
        "server": [
            "namemaker",
            "sqlalchemy",
            "sqlalchemy-json",
            "psycopg2-binary",
            "alembic[tz]",
            "fastapi",
            "uvicorn[standard]",
        ]
    },
    # entry_points={
    #     "console_scripts": [
    #         "hybot = hybot:Hybot.main"
    #     ]
    # }
)
