from setuptools import setup, find_packages
# Instructions for build & install in terminal:
# > cd /path/to/proc_fsds/
# > python setup.py sdist bdist_wheel
# > pip install -e ../proc_fsds
setup(
    name="fsds_proc",
    version="0.1.1",
    author="Guy Litt",
    author_email="guy.litt@noaa.gov",
    description="A simple package for processing data in the formulation selection decision support tool",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)