# get_partition_col_collation

## Location
[src/include/utils/partcache.h:98-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/partcache.h#L98-L103)

## Overview
Returns the collation OID for a specific column in a partition key, used during partition bound validation and string comparison operations.

## Definition
```c
static inline Oid
get_partition_col_collation(PartitionKey key, int col)
```

## Detailed Description
This inline function provides a simple accessor to retrieve the collation object identifier (OID) for a specified column in a partition key. Collations define the rules for comparing and sorting text data, including locale-specific sorting rules and case sensitivity. The function is essential during partition bound transformations to ensure that string-based partition bounds are compared using the correct collation rules that match the partitioning column's collation.

## Parameters / Member Variables
- `key`: PartitionKey structure containing partition metadata including collations for each partitioning column
- `col`: Zero-based column index within the partition key to retrieve the collation OID for

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionKey](../P/PartitionKey.md) (struct type)
- Called from (representative examples):
  - [transformPartitionBound](../t/transformPartitionBound.md)
  - [transformPartitionRangeBounds](../t/transformPartitionRangeBounds.md)

## Notes and Other Information
- This is a static inline function defined in the header file for performance optimization
- The function assumes the caller has validated that `col` is within the valid range for the partition key
- Collation is particularly important for text/string partitioning columns to ensure consistent sorting and comparison behavior
- The returned OID can be used with collation-aware comparison functions
- Used primarily during DDL operations when creating or validating partition bounds for string-based partitioning columns