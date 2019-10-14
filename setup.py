from setuptools import setup, find_namespace_packages
  
install_requires=["pyyaml", "pika"]

setup(
    name="dm_ATArchiver",
    version="1.0.0",
    description="ATArchiver CSC and support classes",
    install_requires=install_requires,
    package_dir={"": "python"},
    packages=find_namespace_packages(where="python"),
    scripts=["bin/run_at_archiver_csc.py", "bin/run_archiver_controller.py", "bin/atevent.py"],
    license="GPL",
    project_urls={
        "Bug Tracker": "https://jira.lsstcorp.org/secure/Dashboard.jspa",
        "Source Code": "https://github.com/lsst-dm/dm_ATArchiver",
    }
)
