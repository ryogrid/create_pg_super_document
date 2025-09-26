# pgstat_relation_delete_pending_cb

## Location
[src/backend/utils/activity/pgstat_relation.c:885-897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L885-L897)

## Overview
Callback function that performs cleanup when deleting a pending relation statistics entry, ensuring proper unlinking of relation references.

## Definition
```c
void pgstat_relation_delete_pending_cb(PgStat_EntryRef *entry_ref)
```

## Detailed Description
This is a cleanup callback function used by PostgreSQL's statistics collection system when a pending relation statistics entry is being deleted. The function's primary responsibility is to properly unlink any relation references that were maintained in the pending statistics entry to prevent memory leaks and maintain referential integrity.

The function checks if the pending statistics entry has an associated relation reference and, if so, calls pgstat_unlink_relation to properly clean up that reference. This ensures that when statistics entries are removed (due to relation drops, transaction rollbacks, or other cleanup scenarios), all associated resources are properly released.

## Parameters / Member Variables
- `entry_ref`: Reference to the statistics entry being deleted, containing the pending statistics data

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_unlink_relation](pgstat_unlink_relation.md)
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md) (data structure)
  - [PgStat_TableStatus](../P/PgStat_TableStatus.md) (data structure)
- Called from (representative examples):
  - Statistics hash table management system (SH_DECLARE in pgstat.c)

## Notes and Other Information
- This is a callback function registered with the statistics hash table management system
- Ensures proper cleanup of relation references when statistics entries are deleted
- Part of PostgreSQL's resource management for the statistics collection system
- Works in conjunction with other statistics management callbacks to maintain system integrity
- The function is simple but critical for preventing resource leaks in the statistics subsystem