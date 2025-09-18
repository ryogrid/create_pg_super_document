# DropRelationCallbackState

## Location
src/backend/commands/tablecmds.c: 311 - 324

## Overview
A communication structure used between `RemoveRelations` and `RangeVarCallbackForDropRelation` to coordinate relation dropping operations and track subsidiary locks during the DROP statement execution.

## Definition
```c
struct DropRelationCallbackState
{
    /* These fields are set by RemoveRelations: */
    char        expected_relkind;
    LOCKMODE    heap_lockmode;
    /* These fields are state to track which subsidiary locks are held: */
    Oid         heapOid;
    Oid         partParentOid;
    /* These fields are passed back by RangeVarCallbackForDropRelation: */
    char        actual_relkind;
    char        actual_relpersistence;
};
```

## Detailed Description
This structure facilitates communication during DROP operations (TABLE, INDEX, SEQUENCE, VIEW, MATERIALIZED VIEW, FOREIGN TABLE) by maintaining state information between the main `RemoveRelations` function and its callback `RangeVarCallbackForDropRelation`. It serves as a coordination mechanism to ensure proper locking order and validation during relation dropping, particularly important for avoiding deadlocks when dropping indexes (which must lock their parent table first) and partitions (which must lock their parent before the partition itself).

The structure tracks expected vs actual relation properties, manages subsidiary locks on related objects, and ensures that the callback function can provide feedback to the main removal logic about the actual characteristics of the relation being dropped.

## Parameters / Member Variables
- `expected_relkind`: The type of relation expected to be dropped (set by RemoveRelations based on DROP statement type)
- `heap_lockmode`: The lock mode to use when locking the parent table (ShareUpdateExclusiveLock for concurrent operations, AccessExclusiveLock otherwise)
- `heapOid`: OID of the heap table that needs to be locked (for index drops), InvalidOid if no heap lock is held
- `partParentOid`: OID of the partition parent that needs to be locked (for partition drops), InvalidOid if no parent lock is held
- `actual_relkind`: The actual relation kind found by the callback (returned by RangeVarCallbackForDropRelation)
- `actual_relpersistence`: The actual persistence characteristic of the relation (returned by RangeVarCallbackForDropRelation)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (data type)
  - LOCKMODE (data type)
- Called from (representative examples):
  - [RemoveRelations](../R/RemoveRelations.md) (src/backend/commands/tablecmds.c:1544)
  - [RangeVarCallbackForDropRelation](../R/RangeVarCallbackForDropRelation.md) (src/backend/commands/tablecmds.c:1636, 1643)

## Notes and Other Information
- Critical for preventing deadlocks during DROP operations by ensuring proper lock acquisition order
- Handles both regular relations and partitioned relations/indexes with appropriate parent locking
- The state tracking prevents redundant lock operations when the same relation is referenced multiple times
- Used specifically in the context of DROP statement execution where relation resolution and locking must be carefully coordinated