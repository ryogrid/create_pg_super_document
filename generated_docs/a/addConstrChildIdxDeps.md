# addConstrChildIdxDeps

## Location
[src/bin/pg_dump/pg_dump.c:7987-8009](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L7987-L8009)

## Overview
A recursive subroutine for getConstraints that establishes dependency relationships between foreign key constraints and partitioned index attachments to ensure proper restoration order during pg_dump operations.

## Definition

```c
static void
addConstrChildIdxDeps(DumpableObject *dobj, const IndxInfo *refidx)
```
## Detailed Description
This function is a critical component of pg_dump's constraint handling system. It recursively traverses a partitioned index's partition attachments and marks a foreign key constraint object as dependent on each partition's DO_INDEX_ATTACH object. This dependency system ensures that during database restoration, foreign key constraints are not restored until all referenced indexes are fully validated and attached.

The function operates by walking through the partition attachment list of a referenced index and adding dependencies from the constraint object to each partition's index attachment object. If a partition itself has sub-partitions, the function recursively processes those as well, creating a complete dependency tree that mirrors the partitioning hierarchy.

## Parameters / Member Variables
- `*dobj`: A DumpableObject representing the foreign key constraint that needs dependencies established
- `*refidx`: A constant pointer to IndxInfo representing the partitioned index being referenced by the foreign key
## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (struct type)
  - [IndxInfo](../I/IndxInfo.md) (struct type)
  - [SimplePtrListCell](../S/SimplePtrListCell.md) (struct type)
  - DO_FK_CONSTRAINT (enum value)
  - [IndexAttachInfo](../I/IndexAttachInfo.md) (struct type)
  - [addObjectDependency](addObjectDependency.md) (function)
  - [addConstrChildIdxDeps](addConstrChildIdxDeps.md) (recursive self-call)

- Called from (representative examples):
  - [getConstraints](../g/getConstraints.md) (primary caller)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_dump.c file
- The function includes an assertion to verify that the passed object is indeed a foreign key constraint (DO_FK_CONSTRAINT)
- The recursive nature handles arbitrarily deep partitioning hierarchies
- This dependency management is crucial for maintaining referential integrity during database restoration
- The function works specifically with partitioned indexes and their attachment objects, which are part of PostgreSQL's declarative partitioning feature

## Simplified Source

```c
static void addConstrChildIdxDeps(DumpableObject *dobj, const IndxInfo *refidx)
{
    SimplePtrListCell *cell;

    Assert(dobj->objType == DO_FK_CONSTRAINT);

    // Walk through all partition attachments of the referenced index
    for (cell = refidx->partattaches.head; cell; cell = cell->next)
    {
        IndexAttachInfo *attach = (IndexAttachInfo *) cell->ptr;

        // Add dependency from FK constraint to this partition's index attachment
        // This ensures FK won't be restored until index is fully validated
        addObjectDependency(dobj, attach->dobj.dumpId);

        // Recursively handle sub-partitions if they exist
        if (attach->partitionIdx->partattaches.head != NULL)
            addConstrChildIdxDeps(dobj, attach->partitionIdx);
    }
}
```