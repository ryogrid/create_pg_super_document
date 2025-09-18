# CheckTableNotInUse

## Location
src/backend/commands/tablecmds.c: 4281 - 4313

## Overview
Verifies that a relation is not actively being used by the current backend session before allowing potentially destructive operations like ALTER TABLE, preventing concurrent access conflicts and data corruption.

## Definition
```c
void CheckTableNotInUse(Relation rel, const char *stmt)
```

## Detailed Description
This function serves as a critical safety mechanism that prevents ALTER TABLE and similar DDL commands from executing when the target relation is actively being used by the current backend session. It performs two main safety checks:

1. **Reference Count Check**: Verifies that the relation's reference count matches the expected value (1 for normal relations, 2 for nailed relations). If the reference count is higher, it indicates there are open cursors, active plans, or other open references to the relation that could be invalidated by the ALTER operation.

2. **Pending Trigger Events Check**: For non-index relations, it checks whether there are any pending AFTER trigger events. This is essential because table-rewriting operations don't preserve tuple TIDs, which would cause pending trigger events to reference invalid tuples.

The function is designed to "err on the side of paranoia" to prevent data corruption and ensure transaction safety. While some ALTER operations might be theoretically safe with concurrent access, the function takes a conservative approach to maintain data integrity.

## Parameters / Member Variables
- `rel`: The relation to be checked for active usage
- `stmt`: The name of the SQL statement being executed (e.g., "ALTER TABLE") for use in error messages

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelationName (gets the relation name for error messages)
  - RelationGetRelid (gets the relation OID)
  - AfterTriggerPendingOnRel (checks for pending AFTER trigger events)
  - ereport (reports errors with appropriate error codes)
  - RELKIND_INDEX, RELKIND_PARTITIONED_INDEX (relation kind constants)
- Called from (representative examples):
  - [CheckAlterTableIsSafe](CheckAlterTableIsSafe.md) (in tablecmds.c:4331, as part of ALTER TABLE safety checks)
  - [cluster_rel](../c/cluster_rel.md) (in cluster.c:444, before clustering a relation)
  - [reindex_index](../r/reindex_index.md) (in index.c:3707, before reindexing)
  - [truncate_check_activity](../t/truncate_check_activity.md) (in tablecmds.c:2383, before truncating)
  - [DefineIndex](../D/DefineIndex.md) (in indexcmds.c:748, during index creation)

## Notes and Other Information
- The function distinguishes between "nailed" relations (critical system catalogs that are kept permanently open) and regular relations when checking reference counts
- For index relations, the trigger event check is skipped since index operations don't affect the parent table's tuple structure
- Uses ERRCODE_OBJECT_IN_USE error code to provide clear diagnostic information
- This safety mechanism is particularly important for operations like ALTER COLUMN TYPE that require table rewrites
- The function helps prevent "stomping on our own foot" scenarios where a backend could interfere with its own ongoing operations
- Part of PostgreSQL's broader strategy to ensure DDL operation safety and prevent data corruption during concurrent access