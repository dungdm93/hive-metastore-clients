`hive-metastore`
================
Python client for Hive metastore (HMS)

**NOTE**:

* parameters always list from smaller scope object to larger scope object,
  e.g (`column`, `partition`, `constraint`) > `table` > `database` > `catalog`.
  This is look like the invert order of Java version of `HiveMetaStoreClient`.
  However, because some methods has default value of `catalog`  and `database`,
  this will make API look more consistent.
  If you prefer forward order, please use keyword arguments like:
    ```python
    client.get_table(catalog_name="foobar", database_name="default", table_name="employee") 
    ```
