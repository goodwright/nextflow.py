from setuptools import setup
from glob import glob
import os

with open("README.md") as f:
    long_description = f.read()

setup(
    name="nextflow",
    version="0.1.2",
    description="A Python wrapper around Nextflow.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/goodwright/nextflow.py",
    author="Sam Ireland",
    author_email="sam@goodwright.com",
    license="GPLv3+",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Internet :: WWW/HTTP",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    keywords="nextflow bioinformatics pipeline",
    packages=["nextflow"],
    python_requires="!=2.*, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=3.5.*",
    install_requires=[]
)