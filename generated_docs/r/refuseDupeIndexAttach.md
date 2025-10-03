# refuseDupeIndexAttach

## Location
[src/backend/commands/tablecmds.c:20004-20026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L20004-L20026)

## Overview
A validation function that prevents duplicate index attachments by checking if a partition table already has an index attached to the specified parent partitioned index.

## Definition

```c
static void
refuseDupeIndexAttach(Relation parentIdx, Relation partIdx, Relation partitionTbl)
```
## Detailed Description
This function serves as a guard against duplicate index attachments in partition hierarchies. It checks whether the given partition table already contains an index that is attached to the specified parent partitioned index. If such an index is found, the function raises an error to prevent the duplicate attachment.

The validation is crucial because each partition can have only one index attached to any given parent partitioned index. Allowing duplicates would violate the partitioning constraints and could lead to inconsistent query planning and execution.

The function uses index_get_partition() to search for an existing index on the partition table that is already a child of the parent index. If found, it generates a detailed error message indicating the conflict.

## Parameters / Member Variables
- : The parent partitioned index relation to which attachment is being attempted
- : The partition index relation that is being attached (used only for error reporting)
- : The partition table relation to check for existing index attachments

## Dependencies
- Functions called/Symbols referenced:
  - [index_get_partition](../i/index_get_partition.md)
  - RelationGetRelid
  - RelationGetRelationName
  - ereport
- Called from (representative examples):
  - [ATExecAttachPartitionIdx](../A/ATExecAttachPartitionIdx.md)

## Notes and Other Information
- This is a static validation function used specifically during index attachment operations
- The function is designed to fail fast - it immediately raises an error if a duplicate is detected
- Error reporting includes specific relation names to help users identify the conflicting objects
- The function uses ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE to indicate the precondition violation
- This validation helps maintain the integrity of the partitioned index hierarchy
- The function is called before any actual attachment operations to prevent partial state changes

## Simplified Source
```c
static void refuseDupeIndexAttach(Relation parentIdx, Relation partIdx, Relation partitionTbl) {
    Oid existingIdx;

    // Check if partition already has an index attached to this parent
    existingIdx = index_get_partition(partitionTbl, RelationGetRelid(parentIdx));

    if (OidIsValid(existingIdx)) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("cannot attach index \"%s\" as a partition of index \"%s\"",
                        RelationGetRelationName(partIdx),
                        RelationGetRelationName(parentIdx)),
                 errdetail("Another index is already attached for partition \"%s\".",
                          RelationGetRelationName(partitionTbl))));
    }
}
```