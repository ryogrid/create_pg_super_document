# pg_stat_have_stats

## Location
[src/backend/utils/adt/pgstatfuncs.c:2026-2034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L2026-L2034)

## Overview
This function checks for the presence of statistics for a specific database object identified by its kind, database OID, and object OID, primarily intended for testing purposes.

## Definition

```c
Datum
pg_stat_have_stats(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function provides a way to verify whether PostgreSQL's statistics collector has data for a specific database object. It accepts three parameters: a statistics type string, a database OID, and an object OID. The function converts the statistics type string to an internal  enumeration value using , then queries the statistics system using  to determine if statistics exist for the specified object.

The function is explicitly noted in the code comments as being useful for testing but not for general application use, and therefore is not documented in the standard PostgreSQL documentation. It returns a boolean value indicating whether statistics are available for the requested object.

## Parameters / Member Variables
-  (text): String representation of the statistics kind (e.g., "relation", "function", etc.)
-  (Oid): Database object identifier for the database containing the object
-  (Oid): Object identifier of the specific database object to check

## Dependencies
- Functions called/Symbols referenced:
  -  - Converts PostgreSQL text type to C string
  -  - Macro to extract text argument from PostgreSQL function call
  -  - Macro to extract OID argument from PostgreSQL function call
  -  - Converts string representation to PgStat_Kind enum
  -  - Enumeration type for different statistics kinds
  -  - Checks if statistics entry exists for the specified object
  -  - Returns boolean value from PostgreSQL function

- Called from (representative examples):
  - No direct references found in the codebase (primarily used for testing)

## Notes and Other Information
- This function is explicitly marked as undocumented and intended primarily for testing purposes
- Part of PostgreSQL's internal statistics system infrastructure
- Located in 
- Not exposed in standard PostgreSQL documentation due to its specialized testing nature
- Useful for verifying that statistics collection is working properly for specific database objects
- The function validates the existence of statistical data without actually retrieving the statistics themselves
- Can be used to test whether statistics have been collected after specific database operations

## Simplified Source

```c
Datum
pg_stat_have_stats(PG_FUNCTION_ARGS)
{
    char *stats_type = text_to_cstring(PG_GETARG_TEXT_P(0));
    Oid db_oid = PG_GETARG_OID(1);
    Oid obj_oid = PG_GETARG_OID(2);

    // Convert string to statistics kind enum
    PgStat_Kind kind = pgstat_get_kind_from_str(stats_type);

    // Check if statistics entry exists for the specified object
    PG_RETURN_BOOL(pgstat_have_entry(kind, db_oid, obj_oid));
}
```