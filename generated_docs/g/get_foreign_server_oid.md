# get_foreign_server_oid

## Location
[src/backend/foreign/foreign.c:704-740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L704-L740)

## Overview
Looks up the OID (Object Identifier) of a foreign server given its name, with optional error handling control.

## Definition

```c
Oid
get_foreign_server_oid(const char *servername, bool missing_ok)
```
## Detailed Description
The `get_foreign_server_oid` function performs a lookup operation to find the OID of a foreign server based on its name. It uses PostgreSQL's system cache mechanism to efficiently retrieve the OID from the `pg_foreign_server` catalog table.

Similar to its counterpart `get_foreign_data_wrapper_oid`, this function provides flexible error handling through the `missing_ok` parameter. When set to false, the function will raise an error if the specified foreign server doesn't exist. When set to true, it will silently return `InvalidOid` instead of raising an error, allowing calling code to handle the missing server case gracefully.

## Parameters / Member Variables
- `servername`: C string containing the name of the foreign server to look up
- `missing_ok`: Boolean flag controlling error behavior:
  - `false`: Raise error if server not found
  - `true`: Return InvalidOid if server not found (no error)

## Dependencies
- Functions called/Symbols referenced:
  - `GetSysCacheOid1`: Retrieves OID from system cache using single key lookup
  - [CStringGetDatum](../C/CStringGetDatum.md): Converts C string to PostgreSQL Datum for cache lookup
  - `OidIsValid`: Checks if the returned OID is valid
  - `ereport`: Reports error when server not found and missing_ok is false
- Called from (representative examples):
  - [objectNamesToOids](../o/objectNamesToOids.md): For ACL (Access Control List) operations
  - [get_object_address_unqualified](get_object_address_unqualified.md): For object address resolution
  - [CreateForeignServer](../C/CreateForeignServer.md): During foreign server creation to check for duplicates
  - [GetForeignServerByName](../G/GetForeignServerByName.md): For retrieving complete foreign server information
  - [convert_server_name](../c/convert_server_name.md): For ACL name conversion operations

## Notes and Other Information
- Uses the `FOREIGNSERVERNAME` cache for efficient lookups
- The lookup is performed on the `pg_foreign_server` system catalog
- Returns `InvalidOid` when the foreign server doesn't exist and `missing_ok` is true
- Error code `ERRCODE_UNDEFINED_OBJECT` is used for missing server errors
- This is a fundamental utility function used throughout the PostgreSQL codebase for foreign server name resolution
- The function is part of the foreign data wrapper infrastructure in PostgreSQL
- Parallel in design and functionality to `get_foreign_data_wrapper_oid` but operates on servers instead of wrappers
- Located in src/backend/foreign/foreign.c:704-740