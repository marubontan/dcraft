from setuptools import setup

from dcraft.__version__ import VERSION

deps = ["pandas", "pyarrow"]
gcp_deps = ["google-cloud-storage>=2.6.0", "google-cloud-bigquery", "pyarrow"]
test_requires = ["pytest"]

setup(
    name="dcraft",
    version=VERSION,
    packages=["dcraft"],
    install_requires=deps,
    test_requires=test_requires,
    extras_require={"gcp": gcp_deps, "test": test_requires},
    author="Shuhei Kishi",
)
