# listener

listener.py
The flask listener for i2b2’s meta data importer


### listener.connection_stats()
Returns the connection stats for database connection (used for
debugging).
Args: None

Returns:

    Response 200 and JSON String of connection stats - see
    DBConnection class for more info.

    Response 504 if database connection not possible


### listener.custom_query(source_id=None)
Use local files to update custom queries


* **Parameters**

    **source_id** ([*Optional*](https://docs.python.org/3/library/typing.html#typing.Optional)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]*) –



### listener.fetch(fuseki_endpoint=None, source_id=None)
Fetch and serialise (as CSV files) the metadata


* **Parameters**


    * **fuseki_endpoint** ([*Optional*](https://docs.python.org/3/library/typing.html#typing.Optional)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]*) –


    * **source_id** ([*Optional*](https://docs.python.org/3/library/typing.html#typing.Optional)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]*) –



### listener.flush(source_id=None)
Flush any existing data with the given source id - both local files and i2b2 database?


* **Parameters**

    **source_id** ([*Optional*](https://docs.python.org/3/library/typing.html#typing.Optional)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]*) –



### listener.load_settings(app_context)
Add the yaml based settings to the app context


### listener.rename(existing_source_id=None, new_source_id=None)
Rename entries with matching source_id


* **Parameters**


    * **existing_source_id** ([*Optional*](https://docs.python.org/3/library/typing.html#typing.Optional)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]*) –


    * **new_source_id** ([*Optional*](https://docs.python.org/3/library/typing.html#typing.Optional)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]*) –



### listener.single(fuseki_endpoint=None, source_id=None)
Run end-to-end for a single source id - no intermediate output


* **Parameters**


    * **fuseki_endpoint** ([*Optional*](https://docs.python.org/3/library/typing.html#typing.Optional)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]*) –


    * **source_id** ([*Optional*](https://docs.python.org/3/library/typing.html#typing.Optional)*[*[*str*](https://docs.python.org/3/library/stdtypes.html#str)*]*) –



### listener.update(source_ids=None)
Push data to i2b2 - read from data which is serialised (as JSON files) by the “fetch” route


* **Parameters**

    **source_ids** ([*Optional*](https://docs.python.org/3/library/typing.html#typing.Optional)*[*[*list*](https://docs.python.org/3/library/stdtypes.html#list)*]*) –