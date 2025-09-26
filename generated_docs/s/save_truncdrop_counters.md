# save_truncdrop_counters

## Location
src/backend/utils/activity/pgstat_relation.c: 963 - 977

## Overview
Saves the current insert/update/delete counter values before a table truncation or drop operation, enabling restoration of these counters if the transaction is later rolled back.

## Definition
```c
static void save_truncdrop_counters(PgStat_TableXactStatus *trans, bool is_drop)
```

## Detailed Description
This function implements a critical part of PostgreSQL's statistics rollback mechanism for destructive table operations. When a table is truncated or dropped within a transaction, the statistics counters (inserted, updated, deleted tuples) are saved so they can be restored if the transaction aborts.

The function operates with the following logic:
1. For drop operations (`is_drop = true`), it always saves the counters regardless of previous state
2. For truncate operations (`is_drop = false`), it only saves counters on the first truncate within a subtransaction level (checked via `truncdropped` flag)
3. The pre-operation counter values are stored in dedicated `*_pre_truncdrop` fields
4. Sets the `truncdropped` flag to indicate that counters have been saved for this transaction level

This design handles the case where multiple truncates might occur in the same subtransaction - only the first truncate's pre-state needs to be preserved for proper rollback.

## Parameters / Member Variables
- `trans`: Pointer to the transaction-specific table statistics status structure containing the counters to save
- `is_drop`: Boolean flag indicating whether this is a drop operation (true) or truncate operation (false)

## Dependencies
- Functions called/Symbols referenced:
  - `PgStat_TableXactStatus`: Transaction-specific table statistics structure containing the counter fields
- Called from (representative examples):
  - `pgstat_drop_relation`: When a relation is being dropped
  - `pgstat_count_truncate`: When a table truncation occurs
  - `AtEOSubXact_PgStat_Relations`: During subtransaction cleanup processing

## Notes and Other Information
- The function is static and only used internally within the statistics relation module
- Drop operations are treated differently from truncate operations - drops always save counters while truncates only save on first occurrence per subtransaction
- This is part of PostgreSQL's transactional statistics system that ensures statistics remain consistent even when transactions containing destructive operations are rolled back
- The saved values are used by `restore_truncdrop_counters` during transaction abort processing
- Supports proper handling of nested subtransactions where truncate/drop operations may occur at different nesting levels