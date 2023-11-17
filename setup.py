import os
from setuptools import setup, find_packages


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "fitextractor",
    version = "0.0.1",
    author = "Jónas Grétar Jónasson",
    author_email = "jonasgretar@gmail.com",
    description = ("A library to load fit files in to sql databases, currently postgres and sqlite."),
    license = "BSD",
    keywords = "database fitness fit",
    url = "https://github.com/jonasinn/fitextractor",
    packages=find_packages(),
    install_requires=[
        'fitdecode>=0.10',
        'numpy>=1.2',
        'pandas>=2.1',
        'SQLAlchemy>=2.0'
    ],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
)