# pg_collation_actual_version

## Location
[src/backend/commands/collationcmds.c:511-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/collationcmds.c#L511-L582)

## Overview
pg_collation_actual_version is a SQL-callable function that returns the actual version string from the underlying collation provider for a given collation OID.

## Definition

```c
Datum
pg_collation_actual_version(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the pg_collation_actual_version() SQL function, which queries the underlying collation library to obtain the current version information for a collation. The function handles two cases:
1. For the default collation (DEFAULT_COLLATION_OID), it retrieves locale information from pg_database
2. For regular collations, it retrieves information from pg_collation

The function determines the appropriate locale string based on the collation provider (libc uses collcollate/datcollate, others use colllocale/datlocale) and then calls get_collation_actual_version() to obtain the version from the provider library.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - TextDatumGetCString
  - [get_collation_actual_version](../g/get_collation_actual_version.md)
  - [cstring_to_text](../c/cstring_to_text.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - SQL queries using pg_collation_actual_version() function

## Notes and Other Information
- Exposed as a SQL function for administrative and diagnostic purposes
- Handles both default collation (from database settings) and regular collations differently
- Provider-aware: uses different catalog columns based on whether the provider is COLLPROVIDER_LIBC or another type
- Returns NULL when the underlying provider doesn't support version information
- Critical for detecting collation version mismatches that could indicate index corruption risks

## Simplified Source

```c
Datum
pg_collation_actual_version(PG_FUNCTION_ARGS)
{
    Oid collid = PG_GETARG_OID(0);
    char provider;
    char *locale;
    char *version;
    Datum datum;

    if (collid == DEFAULT_COLLATION_OID)
    {
        // Get locale info from pg_database for default collation
        HeapTuple dbtup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));

        if (!HeapTupleIsValid(dbtup))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                          errmsg("database with OID %u does not exist", MyDatabaseId)));

        provider = ((Form_pg_database) GETSTRUCT(dbtup))->datlocprovider;

        // Get appropriate locale field based on provider
        if (provider == COLLPROVIDER_LIBC)
            datum = SysCacheGetAttrNotNull(DATABASEOID, dbtup, Anum_pg_database_datcollate);
        else
            datum = SysCacheGetAttrNotNull(DATABASEOID, dbtup, Anum_pg_database_datlocale);

        locale = TextDatumGetCString(datum);
        ReleaseSysCache(dbtup);
    }
    else
    {
        // Get locale info from pg_collation for regular collations
        HeapTuple colltp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));

        if (!HeapTupleIsValid(colltp))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                          errmsg("collation with OID %u does not exist", collid)));

        provider = ((Form_pg_collation) GETSTRUCT(colltp))->collprovider;

        // Get appropriate locale field based on provider
        if (provider == COLLPROVIDER_LIBC)
            datum = SysCacheGetAttrNotNull(COLLOID, colltp, Anum_pg_collation_collcollate);
        else
            datum = SysCacheGetAttrNotNull(COLLOID, colltp, Anum_pg_collation_colllocale);

        locale = TextDatumGetCString(datum);
        ReleaseSysCache(colltp);
    }

    // Get actual version from collation provider
    version = get_collation_actual_version(provider, locale);

    if (version)
        PG_RETURN_TEXT_P(cstring_to_text(version));
    else
        PG_RETURN_NULL();
}
```