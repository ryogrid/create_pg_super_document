# get_partition_exprs

## Location
[src/include/utils/partcache.h:71-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/partcache.h#L71-L79)

## Overview
Returns the list of partition expressions used by a partitioned table for non-simple column partitioning.

## Definition
```c
static inline List *get_partition_exprs(PartitionKey key)
```

## Detailed Description
This inline function provides access to the partition expressions stored in a PartitionKey structure. Partition expressions are used when partitioning is based on expressions rather than simple column references. For example:
- Partitioning by EXTRACT(year FROM date_column)
- Partitioning by UPPER(text_column)
- Partitioning by complex mathematical expressions

The function returns a List of expressions corresponding to each partition key position. For simple column-based partitioning, the corresponding list entry may be NULL, while expression-based partitioning positions contain the actual expression nodes.

## Parameters / Member Variables
- `key`: A PartitionKey structure containing partitioning metadata for a partitioned table

## Dependencies
- Functions called/Symbols referenced:
  - PartitionKey (struct type)
  - List (PostgreSQL list type)
- Called from (representative examples):
  - has_partition_attrs
  - transformPartitionBound
  - transformPartitionRangeBounds

## Notes and Other Information
- This is a static inline function defined in partcache.h for efficient access
- The function returns the partexprs field directly from the PartitionKey structure
- Returns NULL for positions using simple column references
- Contains expression trees for computed partition keys
- Used during partition bound parsing and constraint generation
- Located at src/include/utils/partcache.h:70-74