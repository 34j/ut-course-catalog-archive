=====
Usage
=====

Minimum example:

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

Example with pandas:

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
