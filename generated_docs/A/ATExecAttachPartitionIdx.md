# ATExecAttachPartitionIdx

## Location
[src/backend/commands/tablecmds.c:19849-20003](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L19849-L20003)

## Overview
Implements the ALTER INDEX ATTACH PARTITION command to attach a partition index to a partitioned parent index, establishing the parent-child relationship in the partitioning hierarchy.

## Definition

```c
struct AttachIndexCallbackState state;
```
## Detailed Description
This function handles the complex process of attaching an index on a partition table to its corresponding parent partitioned index. The operation involves multiple validation steps and relationship establishment:

1. **Lock Management**: Carefully acquires locks on the parent index, partition index, and their respective tables to prevent deadlocks and race conditions
2. **Index Resolution**: Uses RangeVarCallbackForAttachIndex to resolve the partition index name and validate it's actually an index
3. **Validation Checks**: Performs comprehensive validation including:
   - Ensuring no duplicate attachments exist
   - Verifying the partition table is actually a partition of the parent table
   - Comparing index definitions for compatibility (columns, collations, operator families)
   - Checking constraint consistency between parent and partition
4. **Relationship Establishment**: Sets up the parent-child relationship for both indexes and any associated constraints
5. **Final Validation**: Validates the complete partitioned index structure

The function is designed to be idempotent - if the attachment already exists in the correct state, it silently succeeds.

## Parameters / Member Variables
- : Work queue for deferred operations (currently unused in this function)
- : The parent partitioned index relation to which the partition index will be attached
- : RangeVar specifying the name of the partition index to be attached

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - [RangeVarCallbackForAttachIndex](../R/RangeVarCallbackForAttachIndex.md)
  - [relation_open](../r/relation_open.md)
  - [relation_close](../r/relation_close.md)
  - [get_partition_parent](../g/get_partition_parent.md)
  - [refuseDupeIndexAttach](../r/refuseDupeIndexAttach.md)
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [CompareIndexInfo](../C/CompareIndexInfo.md)
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md)
  - [free_attrmap](../f/free_attrmap.md)
  - [get_relation_idx_constraint_oid](../g/get_relation_idx_constraint_oid.md)
  - [IndexSetParentIndex](../I/IndexSetParentIndex.md)
  - [ConstraintSetParentConstraint](../C/ConstraintSetParentConstraint.md)
  - [validatePartitionedIndex](../v/validatePartitionedIndex.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (as part of ALTER TABLE command processing)

## Notes and Other Information
- This function implements the core logic for ALTER INDEX ATTACH PARTITION DDL command
- Lock ordering is critical: table locks are acquired before index locks to prevent deadlocks
- The function performs extensive validation to ensure index compatibility, including column mapping through attribute maps
- [Constraint](../C/Constraint.md) inheritance is handled automatically when both parent and partition have associated constraints
- AccessExclusiveLock is used on the partition index to prevent concurrent modifications during attachment
- Error messages are detailed and provide specific information about validation failures
- The function maintains transactional semantics - all changes are committed together or rolled back on error

## Simplified Source

```c
static ObjectAddress
ATExecAttachPartitionIdx(List **wqueue, Relation parentIdx, RangeVar *name)
{
    Relation partIdx, partTbl, parentTbl;
    ObjectAddress address;
    Oid partIdxId, currParent;
    struct AttachIndexCallbackState state;

    // Lock ordering: table before index to prevent deadlocks
    state.partitionOid = InvalidOid;
    state.parentTblOid = parentIdx->rd_index->indrelid;
    state.lockedParentTbl = false;

    // Get partition index with locks
    partIdxId = RangeVarGetRelidExtended(name, AccessExclusiveLock, 0,
                                        RangeVarCallbackForAttachIndex, &state);
    if (!OidIsValid(partIdxId))
        ereport(ERROR, "index \"%s\" does not exist", name->relname);

    // Open all required relations
    partIdx = relation_open(partIdxId, AccessExclusiveLock);
    parentTbl = relation_open(parentIdx->rd_index->indrelid, AccessShareLock);
    partTbl = relation_open(partIdx->rd_index->indrelid, NoLock);

    ObjectAddressSet(address, RelationRelationId, RelationGetRelid(partIdx));

    // Check if already attached to the correct parent
    currParent = partIdx->rd_rel->relispartition ?
        get_partition_parent(partIdxId, false) : InvalidOid;

    if (currParent != RelationGetRelid(parentIdx)) {
        IndexInfo *childInfo, *parentInfo;
        AttrMap *attmap;
        PartitionDesc partDesc;
        Oid constraintOid, cldConstrId = InvalidOid;
        bool found;
        int i;

        // Prevent duplicate attachment
        refuseDupeIndexAttach(parentIdx, partIdx, partTbl);

        if (OidIsValid(currParent))
            ereport(ERROR, "cannot attach index \"%s\" as partition of index \"%s\"",
                    RelationGetRelationName(partIdx), RelationGetRelationName(parentIdx));

        // Verify partition table relationship
        partDesc = RelationGetPartitionDesc(parentTbl, true);
        found = false;
        for (i = 0; i < partDesc->nparts; i++) {
            if (partDesc->oids[i] == state.partitionOid) {
                found = true;
                break;
            }
        }
        if (!found)
            ereport(ERROR, "index \"%s\" is not on any partition of table \"%s\"",
                    RelationGetRelationName(partIdx), RelationGetRelationName(parentTbl));

        // Compare index definitions for compatibility
        childInfo = BuildIndexInfo(partIdx);
        parentInfo = BuildIndexInfo(parentIdx);
        attmap = build_attrmap_by_name(RelationGetDescr(partTbl),
                                      RelationGetDescr(parentTbl), false);

        if (!CompareIndexInfo(childInfo, parentInfo,
                             partIdx->rd_indcollation, parentIdx->rd_indcollation,
                             partIdx->rd_opfamily, parentIdx->rd_opfamily, attmap))
            ereport(ERROR, "cannot attach index - definitions do not match");

        // Handle constraint relationships
        constraintOid = get_relation_idx_constraint_oid(RelationGetRelid(parentTbl),
                                                       RelationGetRelid(parentIdx));
        if (OidIsValid(constraintOid)) {
            cldConstrId = get_relation_idx_constraint_oid(RelationGetRelid(partTbl), partIdxId);
            if (!OidIsValid(cldConstrId))
                ereport(ERROR, "parent index has constraint but partition index does not");
        }

        // Perform the attachment
        IndexSetParentIndex(partIdx, RelationGetRelid(parentIdx));
        if (OidIsValid(constraintOid))
            ConstraintSetParentConstraint(cldConstrId, constraintOid,
                                        RelationGetRelid(partTbl));

        free_attrmap(attmap);
        validatePartitionedIndex(parentIdx, parentTbl);
    }

    // Cleanup and maintain locks until commit
    relation_close(parentTbl, AccessShareLock);
    relation_close(partTbl, NoLock);
    relation_close(partIdx, NoLock);

    return address;
}
```