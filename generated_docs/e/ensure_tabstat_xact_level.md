# ensure_tabstat_xact_level

## Location
src/backend/utils/activity/pgstat_relation.c: 944 - 962

## Overview
Ensures that a transaction-level statistics tracking record exists for a table at the current transaction nesting level, creating one if necessary to support proper rollback of statistics changes.

## Definition
```c
static void ensure_tabstat_xact_level(PgStat_TableStatus *pgstat_info)
```

## Detailed Description
This function serves as a gatekeeper to ensure that transaction-level statistics tracking is properly set up before recording table modifications. It performs a lazy initialization pattern by:

1. Getting the current transaction nesting level using `GetCurrentTransactionNestLevel()`
2. Checking if a transaction record already exists at the current nesting level
3. If no record exists or the existing record is for a different nesting level, calling `add_tabstat_xact_level()` to create the necessary infrastructure

This lazy approach optimizes performance by only creating transaction tracking structures when tables are actually modified within a transaction or subtransaction.

## Parameters / Member Variables
- `pgstat_info`: Pointer to the table's statistics status structure that may need transaction-level tracking

## Dependencies
- Functions called/Symbols referenced:
  - `GetCurrentTransactionNestLevel`: Returns the current transaction nesting depth
  - `add_tabstat_xact_level`: Creates a new transaction state record
  - `PgStat_TableStatus`: Main table statistics tracking structure
- Called from (representative examples):
  - `pgstat_count_heap_insert`: When tracking row insertions
  - `pgstat_count_heap_update`: When tracking row updates  
  - `pgstat_count_heap_delete`: When tracking row deletions
  - `pgstat_count_truncate`: When tracking table truncations

## Notes and Other Information
- The function implements a critical optimization by using lazy initialization - transaction tracking overhead is only incurred when actually needed
- This is called before every table modification operation to ensure proper statistics rollback capability
- The nest level comparison ensures that nested transactions (savepoints) get their own tracking records
- Part of PostgreSQL's MVCC-aware statistics system that must handle complex transaction scenarios including partial rollbacks