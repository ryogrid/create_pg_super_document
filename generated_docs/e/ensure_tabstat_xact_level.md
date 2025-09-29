# ensure_tabstat_xact_level

## Location
[src/backend/utils/activity/pgstat_relation.c:944-962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L944-L962)

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
  - `[GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)`: Returns the current transaction nesting depth
  - `[add_tabstat_xact_level](../a/add_tabstat_xact_level.md)`: Creates a new transaction state record
  - `[PgStat_TableStatus](../P/PgStat_TableStatus.md)`: Main table statistics tracking structure
- Called from (representative examples):
  - `[pgstat_count_heap_insert](../p/pgstat_count_heap_insert.md)`: When tracking row insertions
  - `[pgstat_count_heap_update](../p/pgstat_count_heap_update.md)`: When tracking row updates  
  - `[pgstat_count_heap_delete](../p/pgstat_count_heap_delete.md)`: When tracking row deletions
  - `[pgstat_count_truncate](../p/pgstat_count_truncate.md)`: When tracking table truncations

## Notes and Other Information
- The function implements a critical optimization by using lazy initialization - transaction tracking overhead is only incurred when actually needed
- This is called before every table modification operation to ensure proper statistics rollback capability
- The nest level comparison ensures that nested transactions (savepoints) get their own tracking records
- Part of PostgreSQL's MVCC-aware statistics system that must handle complex transaction scenarios including partial rollbacks

## Simplified Source
```c
static void ensure_tabstat_xact_level(PgStat_TableStatus *pgstat_info) {
    // Get current transaction nesting level
    int nest_level = GetCurrentTransactionNestLevel();

    // Check if we need a new transaction record
    if (pgstat_info->trans == NULL ||
        pgstat_info->trans->nest_level != nest_level) {
        // Create transaction record for this nesting level
        add_tabstat_xact_level(pgstat_info, nest_level);
    }
}
```