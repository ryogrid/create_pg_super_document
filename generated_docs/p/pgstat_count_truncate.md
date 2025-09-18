# pgstat_count_truncate

## Location
src/backend/utils/activity/pgstat_relation.c: 416 - 438

## Overview
Updates PostgreSQL statistics to record a table truncation operation, preserving existing counters and resetting tuple operation counters.

## Definition
```c
void pgstat_count_truncate(Relation rel)
```

## Detailed Description
This function handles statistics updates when a table is truncated. Unlike simple tuple operations, truncation requires special handling because it removes all tuples from a table at once. The function first preserves the current statistics counters by calling `save_truncdrop_counters()`, then resets the transaction-level tuple operation counters (inserted, updated, deleted) to zero.

The preservation of existing counters is important because truncation doesn't negate the work that was done in the current transaction before the truncate operation. The saved counters will be restored if the transaction is rolled back, ensuring statistical accuracy across transaction boundaries.

## Parameters / Member Variables
- `rel`: A `Relation` pointer representing the table being truncated

## Dependencies
- Functions called/Symbols referenced:
  - `pgstat_should_count_relation` - Determines if statistics should be collected for this relation
  - `PgStat_TableStatus` - Structure type for maintaining table-level statistics
  - `ensure_tabstat_xact_level` - Ensures transaction-level statistics tracking is initialized
  - `save_truncdrop_counters` - Preserves current statistics counters before reset

- Called from (representative examples):
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md) - When refreshing materialized views
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md) - Main table truncation execution function

## Notes and Other Information
- This function is part of PostgreSQL's statistics collection framework for DDL operations
- The function preserves existing statistics before resetting counters to handle transaction rollback scenarios properly
- All tuple operation counters (tuples_inserted, tuples_updated, tuples_deleted) are reset to zero after truncation
- The `save_truncdrop_counters` call with `false` parameter indicates this is a truncate operation (not a drop operation)
- Only relations that should have statistics collected will have their counters processed