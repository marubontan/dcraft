from setuptools import find_packages, setup

deps = ["pandas", "pyarrow"]
gcp_deps = ["google-cloud-storage>=2.6.0", "google-cloud-bigquery", "pyarrow"]
test_requires = ["pytest"]

setup(
    name="dcraft",
    version="0.2.0",
    packages=find_packages(exclude=["tests"]),
    install_requires=deps,
    test_requires=test_requires,
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    extras_require={"gcp": gcp_deps, "test": test_requires},
    author="Shuhei Kishi",
)
