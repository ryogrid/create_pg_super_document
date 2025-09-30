# RangeVarCallbackForDropRelation

## Location
[src/backend/commands/tablecmds.c:1632-1790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L1632-L1790)

## Overview
RangeVarCallbackForDropRelation is a callback function that performs permission checks and acquires necessary locks before dropping a relation, ensuring proper authorization and preventing deadlocks through careful lock ordering.

## Definition

```c
struct DropRelationCallbackState *state;
```
## Detailed Description
This function serves as a callback during the relation lookup process for DROP operations. It performs critical safety checks and lock acquisition to ensure the drop operation can proceed safely:

1. **Permission Verification**: Validates that the user has sufficient privileges to drop the relation (either as table owner or schema owner)
2. **Type Validation**: Ensures the relation type matches what is expected for the DROP command
3. **System Catalog Protection**: Prevents dropping system catalogs unless explicitly allowed
4. **Lock Management**: Implements proper lock ordering to prevent deadlocks:
   - For indexes: locks the parent table before the index
   - For partitions: locks the parent partition before the child partition
5. **Invalid Index Handling**: Special handling for invalid system indexes that may need to be dropped after failed concurrent operations

The function also manages cleanup of previously held locks when the relation OID changes between lookups, ensuring no unnecessary locks are maintained.

## Parameters / Member Variables
- : RangeVar representing the relation name being looked up
- : Object ID of the found relation (InvalidOid if not found)
- : Previous relation OID from earlier lookup attempts
- : Pointer to DropRelationCallbackState structure containing callback state

## Dependencies
- Functions called/Symbols referenced:
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [DropErrorMsgWrongType](../D/DropErrorMsgWrongType.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [IsSystemClass](../I/IsSystemClass.md)
  - [IndexGetRelation](../I/IndexGetRelation.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [get_partition_parent](../g/get_partition_parent.md)
- Called from (representative examples):
  - [RemoveRelations](RemoveRelations.md)

## Notes and Other Information
- This callback is specifically designed for DROP operations and implements PostgreSQL's lock ordering rules to prevent deadlocks
- The function handles special cases for partitioned tables/indexes and invalid system indexes
- Permission checks allow either table ownership or schema ownership for DROP operations
- The lock management ensures compatibility with regular query patterns where tables are locked before their indexes and parents before partitions
- System catalog protection can be bypassed with allowSystemTableMods setting for maintenance operations

## Simplified Source

```c
static void RangeVarCallbackForDropRelation(const RangeVar *rel, Oid relOid, Oid oldRelOid, void *arg) {
    HeapTuple tuple;
    struct DropRelationCallbackState *state;
    char expected_relkind;
    bool is_partition;
    Form_pg_class classform;
    LOCKMODE heap_lockmode;

    state = (struct DropRelationCallbackState *) arg;
    heap_lockmode = state->heap_lockmode;

    // Release old locks if relation OID changed
    if (relOid != oldRelOid && OidIsValid(state->heapOid)) {
        UnlockRelationOid(state->heapOid, heap_lockmode);
        state->heapOid = InvalidOid;
    }

    if (relOid != oldRelOid && OidIsValid(state->partParentOid)) {
        UnlockRelationOid(state->partParentOid, AccessExclusiveLock);
        state->partParentOid = InvalidOid;
    }

    // No relation found - nothing to do
    if (!OidIsValid(relOid))
        return;

    // Get relation information from system catalog
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relOid));
    if (!HeapTupleIsValid(tuple))
        return; // Concurrently dropped

    classform = (Form_pg_class) GETSTRUCT(tuple);
    is_partition = classform->relispartition;

    // Save relation information for caller
    state->actual_relkind = classform->relkind;
    state->actual_relpersistence = classform->relpersistence;

    // Handle partitioned table/index type mapping
    if (classform->relkind == RELKIND_PARTITIONED_TABLE)
        expected_relkind = RELKIND_RELATION;
    else if (classform->relkind == RELKIND_PARTITIONED_INDEX)
        expected_relkind = RELKIND_INDEX;
    else
        expected_relkind = classform->relkind;

    // Check if relation type matches expected type
    if (state->expected_relkind != expected_relkind)
        DropErrorMsgWrongType(rel->relname, classform->relkind, state->expected_relkind);

    // Permission check: allow table owner or schema owner
    if (!object_ownercheck(RelationRelationId, relOid, GetUserId()) &&
        !object_ownercheck(NamespaceRelationId, classform->relnamespace, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(classform->relkind), rel->relname);

    // Check for invalid system indexes (special case)
    bool invalid_system_index = false;
    if (IsSystemClass(relOid, classform) && classform->relkind == RELKIND_INDEX) {
        // Check if system index is invalid and can be dropped
        HeapTuple index_tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(relOid));
        if (HeapTupleIsValid(index_tuple)) {
            Form_pg_index indexform = (Form_pg_index) GETSTRUCT(index_tuple);
            if (!indexform->indisvalid)
                invalid_system_index = true;
            ReleaseSysCache(index_tuple);
        }
    }

    // Protect system catalogs (unless invalid index)
    if (!invalid_system_index && !allowSystemTableMods && IsSystemClass(relOid, classform))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied: \"%s\" is a system catalog", rel->relname)));

    ReleaseSysCache(tuple);

    // Lock parent table before index to prevent deadlocks
    if (expected_relkind == RELKIND_INDEX && relOid != oldRelOid) {
        state->heapOid = IndexGetRelation(relOid, true);
        if (OidIsValid(state->heapOid))
            LockRelationOid(state->heapOid, heap_lockmode);
    }

    // Lock parent partition before child to prevent deadlocks
    if (is_partition && relOid != oldRelOid) {
        state->partParentOid = get_partition_parent(relOid, true);
        if (OidIsValid(state->partParentOid))
            LockRelationOid(state->partParentOid, AccessExclusiveLock);
    }
}
```