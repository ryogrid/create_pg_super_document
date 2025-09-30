# get_foreign_data_wrapper_oid

## Location
[src/backend/foreign/foreign.c:681-703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L681-L703)

## Overview
Looks up the OID (Object Identifier) of a foreign data wrapper given its name, with optional error handling control.

## Definition

```c
Oid
get_foreign_data_wrapper_oid(const char *fdwname, bool missing_ok)
```
## Detailed Description
The `get_foreign_data_wrapper_oid` function performs a lookup operation to find the OID of a foreign data wrapper based on its name. It uses PostgreSQL's system cache mechanism to efficiently retrieve the OID from the `pg_foreign_data_wrapper` catalog table.

The function provides flexible error handling through the `missing_ok` parameter. When set to false, the function will raise an error if the specified foreign data wrapper doesn't exist. When set to true, it will silently return `InvalidOid` instead of raising an error, allowing calling code to handle the missing wrapper case gracefully.

## Parameters / Member Variables
- `fdwname`: C string containing the name of the foreign data wrapper to look up
- `missing_ok`: Boolean flag controlling error behavior:
  - `false`: Raise error if FDW not found
  - `true`: Return InvalidOid if FDW not found (no error)

## Dependencies
- Functions called/Symbols referenced:
  - `GetSysCacheOid1`: Retrieves OID from system cache using single key lookup
  - [CStringGetDatum](../C/CStringGetDatum.md): Converts C string to PostgreSQL Datum for cache lookup
  - `OidIsValid`: Checks if the returned OID is valid
  - `ereport`: Reports error when FDW not found and missing_ok is false
- Called from (representative examples):
  - [objectNamesToOids](../o/objectNamesToOids.md): For ACL (Access Control List) operations
  - [get_object_address_unqualified](get_object_address_unqualified.md): For object address resolution
  - [GetForeignDataWrapperByName](../G/GetForeignDataWrapperByName.md): For retrieving complete FDW information
  - [convert_foreign_data_wrapper_name](../c/convert_foreign_data_wrapper_name.md): For ACL name conversion operations

## Notes and Other Information
- Uses the `FOREIGNDATAWRAPPERNAME` cache for efficient lookups
- The lookup is performed on the `pg_foreign_data_wrapper` system catalog
- Returns `InvalidOid` when the foreign data wrapper doesn't exist and `missing_ok` is true
- Error code `ERRCODE_UNDEFINED_OBJECT` is used for missing FDW errors
- This is a fundamental utility function used throughout the PostgreSQL codebase for FDW name resolution
- The function is part of the foreign data wrapper infrastructure in PostgreSQL
- Located in src/backend/foreign/foreign.c:681-703

## Simplified Source

```c
Oid
get_foreign_data_wrapper_oid(const char *fdwname, bool missing_ok)
{
    Oid oid;

    // Look up FDW OID by name in system cache
    oid = GetSysCacheOid1(FOREIGNDATAWRAPPERNAME,
                          Anum_pg_foreign_data_wrapper_oid,
                          CStringGetDatum(fdwname));

    // Handle missing FDW based on missing_ok flag
    if (!OidIsValid(oid) && !missing_ok)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("foreign-data wrapper \"%s\" does not exist",
                        fdwname)));

    return oid;
}
```