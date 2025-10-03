# build_partition_pathkeys

## Location
[src/backend/optimizer/path/pathkeys.c:917-997](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L917-L997)

## Overview
Builds a pathkeys list that describes the ordering induced by the partitions of a partitioned relation, supporting both forward and backward scan directions.

## Definition

```c
List *
build_partition_pathkeys(PlannerInfo *root, RelOptInfo *partrel,
						 ScanDirection scandir, bool *partialkeys)
```
## Detailed Description
This function constructs a list of PathKey objects that represent the sort order implied by the partitioning scheme of a relation. It iterates through each partition key column and attempts to create canonical pathkeys that capture the ordering relationship between partitions.

The function assumes that partitions are properly ordered (verified by partitions_are_ordered()) and handles NULL partition placement by treating scans like NULLS LAST indexes. For each partition key column, it tries to create a pathkey using the partition's operator family, collation, and data type information.

The function stops building pathkeys when it encounters a partition key that cannot be represented as a useful sort order for the current query, unless the key is a boolean constant that can be optimized away.

## Parameters / Member Variables
- `*root`: PlannerInfo containing query planning context and equivalence classes
- `*partrel`: RelOptInfo representing the partitioned relation (must be a simple base relation)
- `scandir`: ScanDirection indicating forward or backward scan direction
- `*partialkeys`: Output parameter set to true if pathkeys only cover a prefix of partition keys, false if all partition key columns are included
## Dependencies
- Functions called/Symbols referenced:
  - [partitions_are_ordered](../p/partitions_are_ordered.md) (to verify partition ordering)
  - IS_SIMPLE_REL (to validate relation type)
  - [make_pathkey_from_sortinfo](../m/make_pathkey_from_sortinfo.md) (to create canonical pathkeys)
  - ScanDirectionIsBackward (to handle scan direction)
  - [pathkey_is_redundant](../p/pathkey_is_redundant.md) (to avoid duplicate pathkeys)
  - [partkey_is_bool_constant_for_query](../p/partkey_is_bool_constant_for_query.md) (to handle boolean partition keys)
- Called from (representative examples):
  - [generate_orderedappend_paths](../g/generate_orderedappend_paths.md)

## Notes and Other Information
- Currently only supports simple base relations (not joins or subqueries)
- Assumes NULL partitions are listed last in the PartitionDesc
- [Boolean](../B/Boolean.md) partition keys receive special treatment and may be skipped if they represent constant conditions
- Part of PostgreSQL's partition-wise join and append optimization system
- The returned pathkeys can be used to determine if an ordered append operation is beneficial

## Simplified Source

```c
List *build_partition_pathkeys(PlannerInfo *root, RelOptInfo *partrel,
                              ScanDirection scandir, bool *partialkeys) {
    List *retval = NIL;
    PartitionScheme partscheme = partrel->part_scheme;
    int i;

    // Iterate through each partition key column
    for (i = 0; i < partscheme->partnatts; i++) {
        PathKey *cpathkey;
        Expr *keyCol = (Expr *) linitial(partrel->partexprs[i]);

        // Try to create canonical pathkey for this partition column
        cpathkey = make_pathkey_from_sortinfo(root,
                                            keyCol,
                                            partscheme->partopfamily[i],
                                            partscheme->partopcintype[i],
                                            partscheme->partcollation[i],
                                            ScanDirectionIsBackward(scandir),  // reverse
                                            ScanDirectionIsBackward(scandir),  // nulls_first
                                            0,
                                            partrel->relids,
                                            false);

        if (cpathkey) {
            // Add pathkey if it's not redundant
            if (!pathkey_is_redundant(cpathkey, retval))
                retval = lappend(retval, cpathkey);
        } else {
            // Stop if key isn't useful, unless it's a boolean constant
            if (!partkey_is_bool_constant_for_query(partrel, i)) {
                *partialkeys = true;
                return retval;
            }
        }
    }

    *partialkeys = false;
    return retval;
}
```