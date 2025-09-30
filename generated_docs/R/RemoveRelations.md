# RemoveRelations

## Location
[src/backend/commands/tablecmds.c:1468-1631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L1468-L1631)

## Overview
RemoveRelations implements the core functionality for DROP TABLE, DROP INDEX, DROP SEQUENCE, DROP VIEW, DROP MATERIALIZED VIEW, and DROP FOREIGN TABLE commands.

## Definition

```c
struct DropRelationCallbackState state;
```
## Detailed Description
RemoveRelations is the main function responsible for handling various DROP statements for database relations. It processes a DropStmt parse tree and coordinates the deletion of one or more relations. The function operates in two phases: first identifying and validating all relations to be dropped, then performing the actual deletions in a single batch operation. It handles special cases like concurrent drops, partitioned indexes, and dependency validation. The function maps different DROP command types to their corresponding relation kinds, performs appropriate locking, validates permissions and constraints, and finally invokes performMultipleDeletions to remove the objects from the system catalogs and file system.

## Parameters / Member Variables
- : DropStmt structure containing the parsed DROP statement with object names, drop behavior, and options

## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [RangeVarGetRelidExtended](RangeVarGetRelidExtended.md)
  - [RangeVarCallbackForDropRelation](RangeVarCallbackForDropRelation.md)
  - [DropErrorMsgNonExistent](../D/DropErrorMsgNonExistent.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [performMultipleDeletions](../p/performMultipleDeletions.md)
  - [free_object_addresses](../f/free_object_addresses.md)
- Called from (representative examples):
  - [ExecDropStmt](../E/ExecDropStmt.md)

## Notes and Other Information
RemoveRelations supports concurrent index dropping with ShareUpdateExclusiveLock, but restricts it to single objects without CASCADE behavior. The function handles partitioned indexes specially by pre-locking all child table partitions to avoid deadlocks. It validates relation types against expected kinds and provides appropriate error messages through helper functions. The two-phase approach (identify first, then delete) prevents unwanted DROP RESTRICT errors when relations have dependencies among themselves. The function processes shared-cache invalidation messages before relation lookups to handle cases where relations were dropped and recreated during the transaction.

## Simplified Source

```c
void RemoveRelations(DropStmt *drop)
{
    // Setup concurrent drop constraints
    LOCKMODE lockmode = drop->concurrent ? ShareUpdateExclusiveLock : AccessExclusiveLock;
    int flags = 0;

    if (drop->concurrent) {
        validate_concurrent_drop_constraints(drop);
    }

    // Map DROP command type to relation kind
    char relkind = map_drop_type_to_relkind(drop->removeType);

    // Phase 1: Identify and validate all relations
    ObjectAddresses *objects = new_object_addresses();

    foreach(cell, drop->objects)
    {
        RangeVar *rel = makeRangeVarFromNameList((List *) lfirst(cell));

        // Flush syscache for dropped/recreated relations
        AcceptInvalidationMessages();

        // Setup callback state for relation validation
        struct DropRelationCallbackState state;
        init_drop_callback_state(&state, relkind, drop->concurrent);

        // Find and lock the relation
        Oid relOid = RangeVarGetRelidExtended(rel, lockmode, RVR_MISSING_OK,
                                              RangeVarCallbackForDropRelation,
                                              &state);

        if (!OidIsValid(relOid)) {
            handle_missing_relation(rel, relkind, drop->missing_ok);
            continue;
        }

        // Handle concurrent drop specifics
        if (drop->concurrent && should_use_concurrent_mode(&state)) {
            flags |= PERFORM_DELETION_CONCURRENTLY;
            validate_concurrent_compatibility(&state, rel);
        }

        // Special handling for partitioned indexes
        if (state.actual_relkind == RELKIND_PARTITIONED_INDEX) {
            prepare_partitioned_index_drop(&state);
        }

        // Add to deletion list
        ObjectAddress obj = {RelationRelationId, relOid, 0};
        add_exact_object_address(&obj, objects);
    }

    // Phase 2: Perform batch deletion
    performMultipleDeletions(objects, drop->behavior, flags);

    // Cleanup
    free_object_addresses(objects);
}

static char map_drop_type_to_relkind(ObjectType removeType)
{
    switch (removeType) {
        case OBJECT_TABLE: return RELKIND_RELATION;
        case OBJECT_INDEX: return RELKIND_INDEX;
        case OBJECT_SEQUENCE: return RELKIND_SEQUENCE;
        case OBJECT_VIEW: return RELKIND_VIEW;
        case OBJECT_MATVIEW: return RELKIND_MATVIEW;
        case OBJECT_FOREIGN_TABLE: return RELKIND_FOREIGN_TABLE;
        default:
            elog(ERROR, "unrecognized drop object type: %d", removeType);
            return 0;
    }
}
```