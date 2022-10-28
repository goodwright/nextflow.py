from setuptools import setup

with open("README.rst") as f:
    long_description = f.read()

setup(
    name="nextflow",
    version="0.5.0",
    description="A Python wrapper around Nextflow.",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/goodwright/nextflow.py",
    author="Sam Ireland",
    author_email="sam@goodwright.com",
    license="GPLv3+",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Internet :: WWW/HTTP",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keywords="nextflow bioinformatics pipeline",
    packages=["nextflow"],
    python_requires="!=2.*, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=3.5.*",
    install_requires=[]
)