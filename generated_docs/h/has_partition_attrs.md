# has_partition_attrs

## Location
src/backend/catalog/partition.c: 255 - 314

## Overview
Checks if any attributes in a given set are used as partition key attributes for a partitioned table, either directly or within partition key expressions.

## Definition
```c
bool has_partition_attrs(Relation rel, Bitmapset *attnums, bool *used_in_expr)
```

## Detailed Description
This function determines whether any attributes from a provided bitmap set are involved in the partitioning scheme of a partitioned table. It handles two types of partition key attributes:

1. **Direct attributes**: Columns that are directly used as partition keys
2. **Expression-based attributes**: Columns that are referenced within partition key expressions

The function iterates through all partition key columns and expressions, checking for matches with the provided attribute set. For direct partition keys, it uses bitmap membership testing. For expression-based keys, it extracts all variable attribute numbers from the expression and checks for overlap with the input set.

## Parameters / Member Variables
- `rel`: The relation to check for partition attributes
- `attnums`: Bitmap set of attribute numbers to check against partition keys
- `used_in_expr`: Output parameter indicating if any matching attribute was found in a partition expression (vs direct use)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetPartitionKey
  - get_partition_natts
  - get_partition_exprs
  - get_partition_col_attnum
  - bms_is_member
  - pull_varattnos
  - bms_overlap
  - list_head, lnext
- Called from (representative examples):
  - ATExecDropColumn
  - ATPrepAlterColumnType
  - expand_partitioned_rtentry

## Notes and Other Information
- Returns false immediately if attnums is NULL or if the relation is not a partitioned table
- The used_in_expr parameter may be ambiguous if a column is used both directly and in expressions - this is acceptable for current use cases as it only affects error message tailoring
- Uses FirstLowInvalidHeapAttributeNumber adjustment for proper attribute number handling in bitmap operations
- Handles both simple column partition keys (partattno != 0) and arbitrary expression partition keys (partattno == 0)