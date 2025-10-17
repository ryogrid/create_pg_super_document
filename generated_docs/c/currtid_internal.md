# currtid_internal

## Location
[src/backend/utils/adt/tid.c:296-335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L296-L335)

## Overview
A utility wrapper function that returns the latest version of a tuple pointing at a specified tuple identifier (TID) for a given relation, with proper access control checks.

## Definition
```c
static ItemPointer currtid_internal(Relation rel, ItemPointer tid)
```

## Detailed Description
The `currtid_internal` function serves as an internal utility wrapper for current CTID (Current Tuple Identifier) operations. It retrieves the latest version of a tuple identified by the given TID within the specified relation. The function performs comprehensive access control checks to ensure the user has SELECT privileges on the relation before proceeding with the operation.

The function handles different relation types appropriately:
- For views (RELKIND_VIEW), it delegates to `currtid_for_view`
- For relations without storage, it raises an error
- For regular tables with storage, it performs a table scan to get the latest TID

The implementation uses a snapshot-based approach to ensure consistent reads and properly manages the scan lifecycle with registration and cleanup of snapshots.

## Parameters / Member Variables
- `rel`: The relation (table/view) containing the tuple
- `tid`: Pointer to the tuple identifier for which to find the latest version

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - [currtid_for_view](currtid_for_view.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - [GetLatestSnapshot](../G/GetLatestSnapshot.md)
  - [RegisterSnapshot](../R/RegisterSnapshot.md)
  - [table_beginscan_tid](../t/table_beginscan_tid.md)
  - [table_tuple_get_latest_tid](../t/table_tuple_get_latest_tid.md)
  - [table_endscan](../t/table_endscan.md)
  - [UnregisterSnapshot](../U/UnregisterSnapshot.md)
- Called from (representative examples):
  - [currtid_for_view](currtid_for_view.md)
  - [currtid_byrelname](currtid_byrelname.md)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the same translation unit
- The function allocates memory for the result ItemPointer using palloc, which is PostgreSQL's memory allocation function
- Access control is enforced at the relation level using ACL_SELECT permission
- The function properly handles different relation kinds and provides appropriate error messages for unsupported operations
- [Snapshot](../S/Snapshot.md) management ensures MVCC (Multi-Version Concurrency Control) compliance during the TID lookup operation

## Simplified Source

```c
static ItemPointer
currtid_internal(Relation rel, ItemPointer tid)
{
    // Allocate result pointer
    ItemPointer result = (ItemPointer) palloc(sizeof(ItemPointerData));

    // Check user has SELECT permission on the relation
    AclResult aclresult = pg_class_aclcheck(RelationGetRelid(rel), GetUserId(), ACL_SELECT);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, get_relkind_objtype(rel->rd_rel->relkind),
                       RelationGetRelationName(rel));

    // Handle different relation types
    if (rel->rd_rel->relkind == RELKIND_VIEW)
        return currtid_for_view(rel, tid);

    if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
        elog(ERROR, "cannot look at latest visible tid for relation \"%s.%s\"",
             get_namespace_name(RelationGetNamespace(rel)),
             RelationGetRelationName(rel));

    // Copy input TID to result
    ItemPointerCopy(tid, result);

    // Get latest version of the tuple using table scan
    Snapshot snapshot = RegisterSnapshot(GetLatestSnapshot());
    TableScanDesc scan = table_beginscan_tid(rel, snapshot);
    table_tuple_get_latest_tid(scan, result);
    table_endscan(scan);
    UnregisterSnapshot(snapshot);

    return result;
}
```