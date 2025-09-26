# MergeAppend

## Location
[src/include/nodes/plannodes.h:287-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L287-L315)

## Overview
The MergeAppend node merges the results of pre-sorted sub-plans to preserve the ordering, commonly used in partitioned table queries where sorted output is required.

## Definition
```c
typedef struct MergeAppend
{
    Plan        plan;
    Bitmapset  *apprelids;          /* RTIs of appendrel(s) formed by this node */
    List       *mergeplans;
    int         numCols;            /* number of sort-key columns */
    AttrNumber *sortColIdx;         /* their indexes in the target list */
    Oid        *sortOperators;      /* OIDs of operators to sort them by */
    Oid        *collations;         /* OIDs of collations */
    bool       *nullsFirst;         /* NULLS FIRST/LAST directions */
    struct PartitionPruneInfo *part_prune_info;
} MergeAppend;
```

## Detailed Description
The MergeAppend execution node is responsible for merging results from multiple pre-sorted child plans while preserving the overall sort order. Unlike the regular Append node which simply concatenates results, MergeAppend performs a merge operation similar to the merge phase of merge sort, ensuring the final output maintains the required ordering.

This node is particularly useful in partitioned table queries where each partition is already sorted according to the same criteria, and the final result needs to maintain that sort order. It efficiently combines the sorted streams from multiple partitions without requiring a separate sort operation on the final result.

## Parameters / Member Variables
- `plan`: Base Plan structure containing common plan node information
- `apprelids`: Bitmapset containing Range Table Indexes (RTIs) of append relations formed by this node
- `mergeplans`: List of child Plan nodes whose sorted results will be merged
- `numCols`: Number of sort-key columns used for merging
- `sortColIdx`: Array of attribute numbers indicating the target list indexes of sort columns
- `sortOperators`: Array of OIDs specifying the operators used for sorting each column
- `collations`: Array of OIDs specifying the collations for each sort column
- `nullsFirst`: Array of boolean values indicating NULLS FIRST/LAST direction for each column
- `part_prune_info`: Pointer to PartitionPruneInfo structure for runtime subplan pruning, NULL if not used

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionPruneInfo](../P/PartitionPruneInfo.md)
- Called from (representative examples):
  - [ExecInitMergeAppend](../E/ExecInitMergeAppend.md)
  - [create_merge_append_plan](../c/create_merge_append_plan.md)
  - [set_mergeappend_references](../s/set_mergeappend_references.md)
  - [show_merge_append_keys](../s/show_merge_append_keys.md)

## Notes and Other Information
- The MergeAppend node assumes all child plans produce results in the same sort order
- It performs an efficient merge operation, similar to merging sorted arrays
- Essential for maintaining sort order in partitioned table queries
- Supports runtime partition pruning to skip unnecessary partitions
- The sort key specification matches that used in Sort nodes for consistency
- Located in src/include/nodes/plannodes.h:287-315