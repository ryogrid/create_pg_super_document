# database_is_invalid_oid

## Location
[src/backend/commands/dbcommands.c:3208-3240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L3208-L3240)

## Overview
A convenience wrapper function that checks whether a database is invalid by looking up its OID and examining its catalog form.

## Definition

```c
struct stat st;
```
## Detailed Description
This function provides a convenient interface to check if a database is in an invalid state using only its OID. It performs a system catalog lookup to retrieve the database's pg_database entry, then calls database_is_invalid_form() to determine validity. The function handles the catalog lookup complexity internally and provides error handling for cases where the database OID doesn't exist. This is particularly useful when you only have a database OID and need to verify the database's validity status.

## Parameters / Member Variables
- : The Object Identifier (Oid) of the database to check for validity

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)  
  - elog (error logging)
  - GETSTRUCT (tuple structure access macro)
  - [database_is_invalid_form](database_is_invalid_form.md) (core validity check)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_database (catalog structure type)
- Called from (representative examples):
  - [createdb](../c/createdb.md) (database creation operations)

## Notes and Other Information
- Wrapper around database_is_invalid_form() that handles OID-to-catalog-entry conversion
- Throws an ERROR if the database OID is not found in the catalog
- Uses system cache for efficient database lookup
- Essential for preventing operations on databases that are being dropped
- Returns the same validity status as database_is_invalid_form() but with OID-based interface
- Part of PostgreSQL's database lifecycle management system

## Simplified Source

```c
bool database_is_invalid_oid(Oid dboid) {
    HeapTuple dbtup;
    Form_pg_database dbform;
    bool invalid;

    // Look up database in system catalog by OID
    dbtup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dboid));
    if (!HeapTupleIsValid(dbtup))
        elog(ERROR, "cache lookup failed for database %u", dboid);

    // Get database form from tuple
    dbform = (Form_pg_database) GETSTRUCT(dbtup);

    // Check if database is invalid using form-based function
    invalid = database_is_invalid_form(dbform);

    // Clean up cache reference
    ReleaseSysCache(dbtup);

    return invalid;
}
```