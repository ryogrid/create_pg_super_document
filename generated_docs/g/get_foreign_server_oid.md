# get_foreign_server_oid

## Location
src/backend/foreign/foreign.c: 704 - 740

## Overview
Looks up the OID (Object Identifier) of a foreign server given its name, with optional error handling control.

## Definition


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
  - `CStringGetDatum`: Converts C string to PostgreSQL Datum for cache lookup
  - `OidIsValid`: Checks if the returned OID is valid
  - `ereport`: Reports error when server not found and missing_ok is false
- Called from (representative examples):
  - `objectNamesToOids`: For ACL (Access Control List) operations
  - `get_object_address_unqualified`: For object address resolution
  - `CreateForeignServer`: During foreign server creation to check for duplicates
  - `GetForeignServerByName`: For retrieving complete foreign server information
  - `convert_server_name`: For ACL name conversion operations

## Notes and Other Information
- Uses the `FOREIGNSERVERNAME` cache for efficient lookups
- The lookup is performed on the `pg_foreign_server` system catalog
- Returns `InvalidOid` when the foreign server doesn't exist and `missing_ok` is true
- Error code `ERRCODE_UNDEFINED_OBJECT` is used for missing server errors
- This is a fundamental utility function used throughout the PostgreSQL codebase for foreign server name resolution
- The function is part of the foreign data wrapper infrastructure in PostgreSQL
- Parallel in design and functionality to `get_foreign_data_wrapper_oid` but operates on servers instead of wrappers
- Located in src/backend/foreign/foreign.c:704-740