# blobapi

> **Disclaimer**: This repository is almost entirely AI-generated, under close human supervision. Everything here is experimental. If any of the experiments prove particularly successful, we may re-implement them in a more designed and "joined up" manner.

blobapi is a member of the [BLOB extension family](https://github.com/phrrngtn/rule4/blob/main/BLOB_EXTENSIONS.md) — four C/C++ libraries (blobtemplates, blobboxes, blobfilters, blobodbc) that share a common pattern of core C API + SQLite/DuckDB/Python wrappers.

This repo deals with web APIs (OpenAPI/Swagger): querying and shredding API repositories to discover what resources are available, then fetching results via `http_enterprise` (in DuckDB) and expanding them into result sets using either `jmespath_search()` or queries with `->>` JSON operators (code-generated from the metadata) to make web API queries look like tables.
