# pgstat_reset

## Location
[src/backend/utils/activity/pgstat.c:733-754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L733-L754)

## Overview
Resets statistics for a single, specific object identified by kind, database OID, and object OID, with optional database timestamp updating for database-scoped statistics.

## Definition

```c
void
pgstat_reset(PgStat_Kind kind, Oid dboid, Oid objoid)
```
## Detailed Description
This function provides fine-grained control over statistics reset operations, allowing the reset of statistics for individual database objects rather than entire databases. It operates on the PostgreSQL statistics system's kind-based architecture, where different types of statistics (tables, functions, subscriptions, etc.) are categorized by their kind.

The function first retrieves metadata about the specified statistics kind to understand how to properly handle the reset operation. It then performs the actual reset of the specific entry and, for database-scoped statistics, also updates the database's reset timestamp to maintain consistency in cumulative statistics calculations.

The function includes safety assertions to ensure it's not called on fixed-amount statistics kinds, as these don't support individual object reset operations with the current function signature.

## Parameters / Member Variables
- : The type of statistics being reset (e.g., table stats, function stats, subscription stats)
- : The OID of the database containing the object
- : The OID of the specific object whose statistics are being reset

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_kind_info](pgstat_get_kind_info.md) (retrieves metadata about the statistics kind)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (obtains current time for reset timestamp)
  - pgstat_reset_entry (performs the actual statistics entry reset)
  - [pgstat_reset_database_timestamp](pgstat_reset_database_timestamp.md) (updates database-level reset timestamp)
- Called from (representative examples):
  - [pg_stat_reset_single_table_counters](pg_stat_reset_single_table_counters.md) (SQL function for table statistics reset)
  - [pg_stat_reset_single_function_counters](pg_stat_reset_single_function_counters.md) (SQL function for function statistics reset)
  - [pg_stat_reset_subscription_stats](pg_stat_reset_subscription_stats.md) (SQL function for subscription statistics reset)
  - pgstat_reset_replslot (replication slot statistics reset)

## Notes and Other Information
- Permission checking is handled through the normal PostgreSQL GRANT system rather than within this function
- The function asserts that it's not called on fixed-amount statistics kinds, as these don't support the current signature
- Database timestamp updating only occurs for statistics kinds that are not accessed across databases
- This provides the granular reset capability that complements the broader pgstat_reset_counters() function
- The function is essential for administrative operations that need to reset statistics for specific objects without affecting the entire database