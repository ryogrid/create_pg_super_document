# get_partition_col_typmod

## Location
[src/include/utils/partcache.h:92-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/partcache.h#L92-L97)

## Overview
Returns the type modifier (typmod) for a specific column in a partition key, used during partition bound validation and type checking.

## Definition

```c
static inline int32
get_partition_col_typmod(PartitionKey key, int col)
```
## Detailed Description
This inline function provides a simple accessor to retrieve the type modifier for a specified column in a partition key. Type modifiers contain additional type information such as precision for numeric types, length limits for character types, or other type-specific constraints. The function is used during partition bound transformations to ensure that partition bounds conform to the expected column types and their modifiers.

## Parameters / Member Variables
- : PartitionKey structure containing partition metadata including type modifiers for each partitioning column
- : Zero-based column index within the partition key to retrieve the type modifier for

## Dependencies
- Functions called/Symbols referenced:
  - PartitionKey (struct type)
- Called from (representative examples):
  - transformPartitionBound
  - transformPartitionRangeBounds

## Notes and Other Information
- This is a static inline function defined in the header file for performance optimization
- The function assumes the caller has validated that  is within the valid range for the partition key
- Type modifiers are essential for ensuring partition bounds match the exact type specifications of partitioning columns
- Used primarily during DDL operations when creating or validating partition bounds