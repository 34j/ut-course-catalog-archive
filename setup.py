#!/usr/bin/env python

"""The setup script."""

from setuptools import find_packages, setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

requirements = ["click>=7.0", "beautifulsoup4>=4.8", "aiohttp>=3.8", "tenacity>=8.1", "aiofiles>=0.7"]

test_requirements = []

setup(
    author="34j",
    author_email="34j@github.com",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    description="Fetch UTokyo Online Course Catalogue.",
    entry_points={
        "console_scripts": [
            "ut_course_catalog=ut_course_catalog.__main__:main",
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme,
    include_package_data=True,
    keywords="ut_course_catalog",
    name="ut_course_catalog",
    packages=find_packages(include=["ut_course_catalog", "ut_course_catalog.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/34j/ut_course_catalog",
    zip_safe=False,
)
