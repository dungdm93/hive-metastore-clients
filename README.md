`hive-metastore` (HMS) clients
==============================

Hive Metastore IDL update based on:
* Thrift: `rel/release-3.1.3`
    * `fb303`: `v0.16.0`

```bash
git apply -v thrift/namespace.patch

thrift -r -gen py --out python/ thrift/hive_metastore.thrift
```
