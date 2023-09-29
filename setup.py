from setuptools import setup
from __version__ import VERSION

with open("requirements.txt") as f:
    required = f.read().splitlines()


setup(
    name="dcraft",
    version=VERSION,
    packages=["dcraft"],
    install_requires=required,
)
