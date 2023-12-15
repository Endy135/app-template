import os
import re
import codecs
from setuptools import setup, find_packages


def _find_version(*file_paths):
    """Read the version number from a source file.

    Why read it, and not import?
    see https://groups.google.com/d/topic/pypa-dev/0PkjVpcxTzQ/discussion

    """
    dirpath = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(dirpath, *file_paths), "r", "latin1") as f:
        version_file = f.read()

    # The version line must have the pattern __version__="ver"
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string")


install_requires = [
    "pypandoc",
    "openpyxl==3.0.6",
    "pyaml==20.4.0",
    "pyarrow==2.0.0",
    "numpy==1.19.5",
    "pandas==1.1.5",
    "pyspark==3.0.1"
]
tests_require = [
    "pytest==6.2.1",
    "pytest-spark==0.6.0",
    "pytest-cov==2.11.1",
    "coverage==5.3.1"
]
docs_require = [
    "sphinx==3.4.3",
    "sphinx_rtd_theme==0.5.1"
]
demos_require = [
    # NOTE: demos require the installation of jupyter
    # but this process is done in scripts/install_jupyter.sh
    # because it requires to configure environment variables
]
extras_require = {
    "tests": install_requires + tests_require,
    "docs": install_requires + docs_require,
    "dev": install_requires + tests_require + docs_require + demos_require,
}


setup(
    name="brc14-app-template",
    version=_find_version("app_template", "__init__.py"),
    description="Common utility functions",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Operating System :: OS Independent",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Topic :: Scientific/Engineering",
    ],
    license="DF",
    author="The Data Factory Team",
    author_email="datafactory@mpsa.com",
    url="",
    download_url="",
    package_data={
        "app_template": ["configuration/resources/*.*"]
    },
    include_package_data=True,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require=extras_require,
    packages=find_packages(),
    entry_points={
        "console_scripts": [
        ]
    }
)
