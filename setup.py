import glob
import os
import sys
from setuptools import setup, find_namespace_packages, setuptools
import pathlib

install_requires=["pyyaml", "pika", "redis"]

tools_path = pathlib.PurePosixPath(setuptools.__path__[0])
base_prefix = pathlib.PurePosixPath(sys.base_prefix)
data_files_path = tools_path.relative_to(base_prefix).parents[1]
schema_files = glob.glob("schema/*.yaml")

setup(
    name="dm_ATArchiver",
    version="1.0.0",
    description="dm csc base classes",
    install_requires=install_requires,
    package_dir={"": "python"},
    packages=find_namespace_packages(where="python"),
    scripts=["bin/run_at_archiver_csc.py", "bin/run_atarchive_controller.py", "bin/run_atarchive_controller.sh", "bin/run_atarchiver.sh", "bin/setup_atarchiver.sh"],
    data_files=[(os.path.join(data_files_path, "schema"), ["schema/ATArchiver.yaml"])],
    include_package_data=True,
    zip_safe=False,
    license="GPL",
    project_urls={
        "Bug Tracker": "https://jira.lsstcorp.org/secure/Dashboard.jspa",
        "Source Code": "https://github.com/lsst-dm/dm_ATArchiver",
    }
)
