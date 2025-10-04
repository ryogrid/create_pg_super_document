# pg_database_collation_actual_version

## Location
[src/backend/commands/dbcommands.c:2737-2780](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L2737-L2780)

## Overview
pg_database_collation_actual_version is a PostgreSQL SQL function that returns the actual collation version for a specified database by querying the system collation library.

## Definition

```c
Datum
pg_database_collation_actual_version(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the SQL function `pg_database_collation_actual_version(database_oid)` which returns the current version string from the system collation library for a given database. The function:

1. Takes a database OID as input via PG_GETARG_OID(0)
2. Looks up the database record in the system cache using SearchSysCache1()
3. Retrieves the collation provider type (datlocprovider) from the database record
4. Based on the provider type, fetches either the datcollate (for COLLPROVIDER_LIBC) or datlocale attribute
5. Calls get_collation_actual_version() to query the system collation library for the current version
6. Returns the version string as a PostgreSQL text datum, or NULL if no version is available

This function is typically used for monitoring collation version changes that might affect index integrity after system collation library updates.

## Parameters / Member Variables
- Input: Database OID (via PG_FUNCTION_ARGS framework)
- Returns: Text datum containing the collation version string, or NULL

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [get_collation_actual_version](../g/get_collation_actual_version.md)
  - TextDatumGetCString
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [cstring_to_text](../c/cstring_to_text.md)
  - PG_RETURN_TEXT_P
  - PG_RETURN_NULL
- Called from (representative examples):
  - SQL queries (exposed as SQL function)

## Notes and Other Information
- Exposed as a SQL-callable function for database administration and monitoring
- Handles both COLLPROVIDER_LIBC and other collation provider types appropriately
- Returns NULL when the collation library doesn't provide version information
- Used in conjunction with stored collation versions in pg_database.datcollversion for detecting mismatches
- Part of PostgreSQL's collation version tracking system for maintaining data integrity
- Function follows PostgreSQL's function call convention using PG_FUNCTION_ARGS framework

## Simplified Source

```c
Datum
pg_database_collation_actual_version(PG_FUNCTION_ARGS)
{
    Oid dbid = PG_GETARG_OID(0);
    HeapTuple tp;
    char datlocprovider;
    Datum datum;
    char *version;

    // Look up database record
    tp = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(dbid));
    if (!HeapTupleIsValid(tp))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("database with OID %u does not exist", dbid)));

    // Get collation provider type
    datlocprovider = ((Form_pg_database) GETSTRUCT(tp))->datlocprovider;

    // Get appropriate locale field based on provider
    if (datlocprovider == COLLPROVIDER_LIBC)
        datum = SysCacheGetAttrNotNull(DATABASEOID, tp, Anum_pg_database_datcollate);
    else
        datum = SysCacheGetAttrNotNull(DATABASEOID, tp, Anum_pg_database_datlocale);

    // Get actual version from collation provider
    version = get_collation_actual_version(datlocprovider, TextDatumGetCString(datum));

    ReleaseSysCache(tp);

    if (version)
        PG_RETURN_TEXT_P(cstring_to_text(version));
    else
        PG_RETURN_NULL();
}
```