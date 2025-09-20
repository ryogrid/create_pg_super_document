# pg_client_encoding

## Location
[src/backend/utils/mb/mbutils.c:1279-1284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1279-L1284)

## Overview
Returns the name of the current client character encoding as a PostgreSQL SQL function that can be called from within SQL queries.

## Definition

```c
Datum
pg_client_encoding(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL SQL function (registered in pg_proc.dat) that returns the name of the current client character encoding. It retrieves the encoding name from the global ClientEncoding variable, which points to an entry in the pg_enc2name_tbl table containing encoding metadata. The function converts the encoding name to a PostgreSQL name data type using the namein function.

The function is marked as 'stable' (provolatile => 's') in the system catalog, meaning it returns the same result for the same parameters within a single statement but may change between statements.

## Parameters / Member Variables
This function takes no parameters (proargtypes => '' in pg_proc.dat).

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall1 - PostgreSQL function call framework
  - namein - converts C string to PostgreSQL name type  
  - [CStringGetDatum](../C/CStringGetDatum.md) - converts C string to PostgreSQL Datum
  - ClientEncoding - global variable pointing to current client encoding info

- Called from (representative examples):
  - SQL queries via PostgreSQL function call mechanism
  - No direct C code references found

## Notes and Other Information
- The function is registered in the PostgreSQL system catalog as a SQL-callable function
- ClientEncoding is a static global variable in mbutils.c that points to the current encoding entry
- The encoding name returned comes from the pg_enc2name_tbl table structure
- Function signature location: src/backend/utils/mb/mbutils.c:1279-1284
- Catalog definition: src/include/catalog/pg_proc.dat:3753-3754