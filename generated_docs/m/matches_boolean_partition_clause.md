# matches_boolean_partition_clause

## Location
[src/backend/optimizer/path/pathkeys.c:882-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L882-L916)

## Overview
Determines if a boolean clause matches a specific partition key column, supporting both direct matches (partkey = true) and NOT matches (partkey = false).

## Definition

```c
static bool
matches_boolean_partition_clause(RestrictInfo *rinfo,
								 RelOptInfo *partrel, int partkeycol)
```
## Detailed Description
This function analyzes a boolean restriction clause to determine if it matches a partition key column in a partitioned table. It supports two types of matches:
1. Direct match: The clause expression is equivalent to "partkey = true"
2. NOT match: The clause is a NOT expression equivalent to "partkey = false"

The function extracts the partition expression from the specified column and compares it against the restriction clause using structural equality checking. For NOT clauses, it examines the argument within the NOT to check for equality with the partition expression.

## Parameters / Member Variables
- : RestrictInfo containing the boolean clause to be matched
- : RelOptInfo representing the partitioned relation
- : Zero-based index of the partition key column to match against

## Dependencies
- Functions called/Symbols referenced:
  - [equal](../e/equal.md) (for structural equality comparison)
  - [is_notclause](../i/is_notclause.md) (to check if clause is a NOT expression)
  - [get_notclausearg](../g/get_notclausearg.md) (to extract argument from NOT clause)
- Called from (representative examples):
  - [partkey_is_bool_constant_for_query](../p/partkey_is_bool_constant_for_query.md)

## Notes and Other Information
- This is a static helper function used internally within the pathkeys module
- The function performs structural equality checks rather than semantic equivalence
- Designed specifically for boolean partition key columns where the partition key itself acts as a boolean expression
- Part of PostgreSQL's partition pruning optimization system