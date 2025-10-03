# AttachPartitionEnsureIndexes

## Location
[src/backend/commands/tablecmds.c:18803-18983](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L18803-L18983)

## Overview
AttachPartitionEnsureIndexes ensures that a partition being attached has all the required indexes that match the partitioned indexes on the parent table, either by finding existing compatible indexes or creating new ones.

## Definition
```c
static void AttachPartitionEnsureIndexes(List **wqueue, Relation rel, Relation attachrel)
```

## Detailed Description
This function enforces PostgreSQL's indexing rule for partitioned tables: every partition must have an index attached to each partitioned index on the parent table. The function operates in several phases:

**Index Discovery Phase:**
- Retrieves all indexes from both the parent partitioned table and the partition being attached
- Opens all existing indexes on the partition and builds IndexInfo structures for comparison
- Uses a temporary memory context for efficient memory management during the process

**Foreign Table Handling:**
- For foreign tables, validates that the parent has no unique or primary key indexes, as these cannot be enforced on foreign partitions
- Exits early for foreign tables after validation since no index creation is possible

**Index Matching and Creation Phase:**
For each partitioned index on the parent table:
- Searches for a compatible existing index on the partition using CompareIndexInfo
- Validates that both parent and child have matching constraint types if the index enforces a constraint
- If a compatible index is found, establishes the parent-child relationship via IndexSetParentIndex
- If no compatible index exists, creates a new index using generateClonedIndexStmt and DefineIndex

The function uses attribute mapping to handle differences in column ordering between parent and partition tables.

## Parameters / Member Variables
- `wqueue`: Work queue for ALTER TABLE operations (currently unused in this function)
- `rel`: The parent partitioned table relation
- `attachrel`: The partition relation being attached

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate, RelationGetIndexList, index_open, BuildIndexInfo
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md), get_relation_idx_constraint_oid, CompareIndexInfo
  - [get_constraint_type](../g/get_constraint_type.md), IndexSetParentIndex, ConstraintSetParentConstraint
  - [generateClonedIndexStmt](../g/generateClonedIndexStmt.md), DefineIndex, CommandCounterIncrement
  - [index_close](../i/index_close.md), MemoryContextDelete
- Called from (representative examples):
  - [ATExecAttachPartition](ATExecAttachPartition.md)
  - child_dependency_type

## Notes and Other Information
- Static function used as a subroutine of ATExecAttachPartition
- Only processes partitioned indexes (RELKIND_PARTITIONED_INDEX), ignoring regular indexes on the parent
- Prevents attachment of foreign tables with unique indexes on the parent, as uniqueness cannot be enforced across foreign partitions  
- Uses AccessShareLock on indexes during the matching process to allow concurrent reads
- Creates a temporary memory context to manage memory efficiently during index processing
- Increments command counter after establishing parent-child relationships to ensure visibility
- Handles both constraint and non-constraint indexes appropriately
- Critical for maintaining index consistency across the partition hierarchy

## Simplified Source
```c
static void AttachPartitionEnsureIndexes(List **wqueue, Relation rel, Relation attachrel) {
    List *parent_indexes;
    List *partition_indexes;
    Relation *partition_index_rels;
    IndexInfo **partition_infos;

    // Get indexes from both parent and partition
    parent_indexes = RelationGetIndexList(rel);
    partition_indexes = RelationGetIndexList(attachrel);

    // Build array of partition index relations and their info
    partition_index_rels = palloc(sizeof(Relation) * list_length(partition_indexes));
    partition_infos = palloc(sizeof(IndexInfo *) * list_length(partition_indexes));

    foreach_oid(idx_oid, partition_indexes) {
        int i = foreach_current_index(idx_oid);
        partition_index_rels[i] = index_open(idx_oid, AccessShareLock);
        partition_infos[i] = BuildIndexInfo(partition_index_rels[i]);
    }

    // Foreign tables: check for unique indexes and exit if found
    if (attachrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
        foreach(cell, parent_indexes) {
            Oid idx = lfirst_oid(cell);
            Relation idx_rel = index_open(idx, AccessShareLock);

            if (idx_rel->rd_index->indisunique || idx_rel->rd_index->indisprimary) {
                ereport(ERROR, "cannot attach foreign table with unique indexes");
            }
            index_close(idx_rel, AccessShareLock);
        }
        goto cleanup;
    }

    // For each partitioned index on parent, find or create matching index on partition
    foreach(cell, parent_indexes) {
        Oid parent_idx = lfirst_oid(cell);
        Relation parent_idx_rel = index_open(parent_idx, AccessShareLock);
        bool found_match = false;

        // Skip non-partitioned indexes
        if (parent_idx_rel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX) {
            index_close(parent_idx_rel, AccessShareLock);
            continue;
        }

        IndexInfo *parent_info = BuildIndexInfo(parent_idx_rel);
        AttrMap *attr_map = build_attrmap_by_name(RelationGetDescr(attachrel),
                                                 RelationGetDescr(rel), false);
        Oid parent_constraint = get_relation_idx_constraint_oid(RelationGetRelid(rel), parent_idx);

        // Search for compatible existing index on partition
        for (int i = 0; i < list_length(partition_indexes); i++) {
            // Skip if already has parent or is invalid
            if (partition_index_rels[i]->rd_rel->relispartition ||
                !partition_index_rels[i]->rd_index->indisvalid) {
                continue;
            }

            // Check if indexes are compatible
            if (CompareIndexInfo(partition_infos[i], parent_info,
                               partition_index_rels[i]->rd_indcollation,
                               parent_idx_rel->rd_indcollation,
                               partition_index_rels[i]->rd_opfamily,
                               parent_idx_rel->rd_opfamily, attr_map)) {

                // Verify constraint compatibility if needed
                if (OidIsValid(parent_constraint)) {
                    Oid child_constraint = get_relation_idx_constraint_oid(
                        RelationGetRelid(attachrel),
                        RelationGetRelid(partition_index_rels[i]));

                    if (!OidIsValid(child_constraint) ||
                        get_constraint_type(parent_constraint) != get_constraint_type(child_constraint)) {
                        continue;
                    }
                }

                // Attach index to parent
                IndexSetParentIndex(partition_index_rels[i], parent_idx);
                if (OidIsValid(parent_constraint)) {
                    ConstraintSetParentConstraint(child_constraint, parent_constraint,
                                                RelationGetRelid(attachrel));
                }
                found_match = true;
                CommandCounterIncrement();
                break;
            }
        }

        // Create new index if no match found
        if (!found_match) {
            IndexStmt *stmt;
            Oid constraint_oid;

            stmt = generateClonedIndexStmt(NULL, parent_idx_rel, attr_map, &constraint_oid);
            DefineIndex(RelationGetRelid(attachrel), stmt, InvalidOid,
                       RelationGetRelid(parent_idx_rel), constraint_oid,
                       -1, true, false, false, false, false);
        }

        index_close(parent_idx_rel, AccessShareLock);
    }

cleanup:
    // Clean up partition index relations
    for (int i = 0; i < list_length(partition_indexes); i++) {
        index_close(partition_index_rels[i], AccessShareLock);
    }
}
```