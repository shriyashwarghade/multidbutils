import io
import os
import re

from setuptools import find_packages
from setuptools import setup


def read(filename):
    filename = os.path.join(os.path.dirname(__file__), filename)
    text_type = type(u"")
    with io.open(filename, mode="r", encoding='utf-8') as fd:
        return re.sub(text_type(r':[a-z]+:`~?(.*?)`'), text_type(r'``\1``'), fd.read())

setup(
    name="multidbutils",
    version="1.0.0",
    url="https://github.com/shriyashwarghade/multidbutils",
    license='MIT',

    author="Shriyash Warghade",
    author_email="warghade.shriyash@gmail.com",

    description="Python package for Exporting, Importing, Sync multiple Databases.",
    long_description=read("README.rst"),

    packages=find_packages(exclude=('tests',),
                           include=(
                               "multidbutils",
                               "multidbutils._core.functions",
                               "multidbutils._core.mssql",
                               "multidbutils._webserver",
                           )),

    install_requires=[
        "tqdm", "pyodbc", "pymongo", "fastapi", "uvicorn"
    ],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],

    entry_points={
        "console_scripts": [
            "multidbutils-server=multidbutils.webserver:main",
        ]
    },
)
