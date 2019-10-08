from setuptools import setup, find_namespace_packages
import glob

bin_files= glob.glob("bin.src/*")
  
install_requires=["pyyaml", "pika"]

setup(
    name="dm_ATArchiver",
    version="1.0.0",
    description="ATArchiver CSC",
    install_requires=install_requires,
    package_dir={"": "python"},
    packages=find_namespace_packages(where="python"),
    scripts=bin_files,
    license="GPL",
    project_urls={
        "Bug Tracker": "https://jira.lsstcorp.org/secure/Dashboard.jspa",
        "Source Code": "https://github.com/lsst-dm/dm_ATArchiver",
    }
)
