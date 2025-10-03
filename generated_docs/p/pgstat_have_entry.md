# pgstat_have_entry

## Location
[src/backend/utils/activity/pgstat.c:924-939](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L924-L939)

## Overview
This function determines whether statistics exist for a specific database object identified by kind, database OID, and object OID.

## Definition

```c
bool
pgstat_have_entry(PgStat_Kind kind, Oid dboid, Oid objoid)
```
## Detailed Description
The  function provides a lightweight way to check for the existence of statistics for a particular database object without actually fetching the statistics data. This function is particularly useful for conditional logic that needs to determine whether statistics collection has been enabled or initialized for a specific object.

The function handles two distinct categories of statistics: fixed-amount statistics (such as shared system statistics) and variable-amount statistics (such as table, function, or database-specific statistics). For fixed-amount statistics, the function always returns true since these statistics are always present once the statistics system is initialized. For variable-amount statistics, it queries the statistics registry to determine if an entry exists for the specified object.

This function serves as an efficient precondition check that can be used before attempting more expensive statistics operations.

## Parameters / Member Variables
- `kind`: A  enum value specifying the type of statistics to check
- `dboid`: The OID of the database containing the object
- `objoid`: The OID of the specific object to check for statistics
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_kind_info](pgstat_get_kind_info.md)
  - [pgstat_get_entry_ref](pgstat_get_entry_ref.md)
  - [PgStat_Kind](../P/PgStat_Kind.md) (enum type)
- Called from (representative examples):
  - [pg_stat_have_stats](pg_stat_have_stats.md)

## Notes and Other Information
- Fixed-amount statistics kinds always return true, as these statistics are always present once initialized
- For variable-amount statistics, the function performs a non-creating lookup to avoid side effects
- This is a read-only operation that does not modify or create statistics entries
- The function provides an efficient way to check statistics existence without the overhead of actually fetching the data
- Particularly useful in SQL functions and administrative tools that need to conditionally display statistics information