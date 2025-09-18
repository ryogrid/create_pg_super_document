# get_non_null_list_datum_count

## Location
[src/backend/partitioning/partbounds.c:436-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L436-L461)

## Overview
Counts the total number of non-null Datum values across all list partition specifications, used for memory allocation sizing in list partition bounds creation.

## Definition
```c
static int get_non_null_list_datum_count(PartitionBoundSpec **boundspecs, int nparts)
```

## Detailed Description
This utility function iterates through an array of PartitionBoundSpec structures representing list partitions and counts the total number of non-null Datum values contained within their listdatums lists. The function is used during list partition bounds creation to determine the appropriate size for memory allocation of the datums array in the PartitionBoundInfo structure.

For each partition specification, the function examines every Const node in the listdatums list and increments the counter only for values that are not marked as null (constisnull = false). This count is essential for properly sizing internal data structures before processing the actual partition bound values.

## Parameters / Member Variables
- `boundspecs`: Array of PartitionBoundSpec pointers containing list partition specifications to examine
- `nparts`: Number of partition specifications in the boundspecs array

## Dependencies
- Functions called/Symbols referenced:
  - foreach (macro for list iteration)
  - lfirst_node (list cell access macro)
  - Const (node type for constant values)
- Called from (representative examples):
  - [create_list_bounds](../c/create_list_bounds.md) (src/backend/partitioning/partbounds.c:482)

## Notes and Other Information
- Static function, only accessible within partbounds.c
- Returns an integer count that can be zero if all list datums are null
- Used specifically for pre-allocation sizing in list partition processing
- Part of the list partition bounds creation pipeline
- Does not modify any input data, purely a counting utility
- Essential for memory management optimization in partition bound creation