import os
from setuptools import find_packages, setup

README_PATH = os.path.join(os.path.dirname(__file__), "README.md")

deps = ["pandas", "pyarrow"]
gcp_deps = ["google-cloud-storage>=2.6.0", "google-cloud-bigquery", "pyarrow"]
mongo_deps = ["pymongo"]
minio_deps = ["minio"]
test_requires = ["pytest"]
all_requirements = deps + gcp_deps + mongo_deps + minio_deps

setup(
    name="dcraft",
    version="0.5.2",
    packages=find_packages(exclude=["tests"]),
    install_requires=deps,
    test_requires=test_requires,
    long_description=open(README_PATH).read(),
    long_description_content_type="text/markdown",
    extras_require={
        "gcp": gcp_deps,
        "mongo": mongo_deps,
        "minio": minio_deps,
        "test": test_requires,
        "all": all_requirements
    },
    author="Shuhei Kishi",
)
