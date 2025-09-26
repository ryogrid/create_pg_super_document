# get_partition_col_attnum

## Location
[src/include/utils/partcache.h:80-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/partcache.h#L80-L85)

## Overview
Returns the attribute number (column number) for a specific partition key column in a partitioned table.

## Definition
```c
static inline int16 get_partition_col_attnum(PartitionKey key, int col)
```

## Detailed Description
This inline function provides access to the attribute numbers of partition key columns stored in a PartitionKey structure. The attribute number identifies which table column is used for partitioning at the specified partition key position. Attribute numbers follow PostgreSQL's standard numbering:
- Positive numbers: regular user-defined columns (1, 2, 3, ...)
- Negative numbers: system columns (like ctid, oid, etc.)
- 0: Invalid/not a simple column reference

This function is essential for identifying which table columns participate in partitioning, allowing the system to extract the correct values from tuples for partition routing and constraint checking.

## Parameters / Member Variables
- `key`: A PartitionKey structure containing partitioning metadata for a partitioned table
- `col`: The partition key column index (0-based) to retrieve the attribute number for

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionKey](../P/PartitionKey.md) (struct type)
- Called from (representative examples):
  - [has_partition_attrs](../h/has_partition_attrs.md)
  - [ExecBuildSlotPartitionKeyDescription](../E/ExecBuildSlotPartitionKeyDescription.md)

## Notes and Other Information
- This is a static inline function defined in partcache.h for efficient access
- The function returns the partattrs[col] field from the PartitionKey structure
- Returns int16 following PostgreSQL's AttrNumber type convention
- Used to map partition key positions to actual table columns
- Essential for tuple routing and partition constraint evaluation
- Located at src/include/utils/partcache.h:76-83