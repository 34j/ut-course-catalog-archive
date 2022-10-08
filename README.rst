=================
ut-course-catalog
=================

.. image:: https://img.shields.io/github/license/34j/ut-course-catalog
        :alt: GitHub License

.. image:: https://img.shields.io/pypi/v/ut_course_catalog.svg
        :target: https://pypi.python.org/pypi/ut_course_catalog

.. image:: https://img.shields.io/travis/34j/ut_course_catalog.svg
        :target: https://travis-ci.com/34j/ut_course_catalog

.. image:: https://readthedocs.org/projects/ut-course-catalog/badge/?version=latest
        :target: https://ut-course-catalog.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/34j/ut_course_catalog/shield.svg
        :target: https://pyup.io/repos/github/34j/ut_course_catalog/
        :alt: Updates

Python package to fetch UTokyo Online Course Catalogue.

Installation
------------

Install ``ut_course_catalog`` using ``pip``:

.. code-block:: shell
        
        pip install ut_course_catalog


Features
--------

* Fetches UTokyo Online Course Catalogue.

Usage
-------

Minimum:

.. code-block:: python

    #1. import
    import ut_course_catalog.ja as utcc

    #2. create a UTCourseCatalog instance
    async with utcc.UTCourseCatalog() as catalog:
        #3. fetch search results
        results = await catalog.fetch_search(utcc.SearchParams(keyword="python"))
        #4. print the results
        print(results)
        
        #3. fetch details
        detail = await catalog.fetch_detail("30001", 2022)
        #4. print the results
        print(detail)

With pandas:

.. code-block:: python

    import pandas as pd
    import ut_course_catalog.ja as utcc

    async with utcc.UTCourseCatalog() as catalog:
        results = await catalog.fetch_search(utcc.SearchParams(keyword="python", 曜日=utcc.Weekday.Mon))
        # convert to pandas DataFrame
        df = pd.DataFrame([x._asdict() for x in results.items])
        display(df)
        
        detail = await catalog.fetch_detail("30001", 2022)
        # convert to pandas DataFrame (not Series, because it is not pretty)
        df = pd.Series(detail._asdict()).to_frame()
        display(df)

For more information, see the `documentation <https://ut-course-catalog.readthedocs.io>`_.
