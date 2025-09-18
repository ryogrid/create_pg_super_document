# partkey_is_bool_constant_for_query

## Location
[src/backend/optimizer/path/pathkeys.c:842-881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L842-L881)

## Overview
Determines if a partition key column is constrained to have a constant boolean value by the query's WHERE conditions, making it irrelevant for sort-order considerations.

## Definition


## Detailed Description
This function addresses a specific optimization challenge with boolean partition key columns. When a partition key column is constrained to a constant value, it becomes irrelevant for sorting purposes. For non-boolean columns, this is typically handled through EquivalenceClasses created from WHERE clauses like "partkeycol = constant". However, boolean columns are special because expression preprocessing simplifies boolean conditions to "WHERE partkeycol" or "WHERE NOT partkeycol" instead of creating explicit equality comparisons.

The function checks if a boolean partition key column has such a boolean restriction clause applied to it, allowing the query planner to treat it as effectively constant for pathkey generation purposes. This ensures boolean partition keys work consistently with non-boolean partition keys in terms of sort optimization.

## Parameters / Member Variables
- : RelOptInfo for the partitioned relation being analyzed
- : Index of the partition key column to check (0-based)

## Dependencies
- Functions called/Symbols referenced:
  - IsBuiltinBooleanOpfamily
  - [matches_boolean_partition_clause](../m/matches_boolean_partition_clause.md)
  - PartitionScheme (type)
- Called from (representative examples):
  - [build_partition_pathkeys](../b/build_partition_pathkeys.md)

## Notes and Other Information
- This is a static function, only used within the pathkeys.c module
- Only works with built-in boolean operator families since partitioning currently only supports built-in access methods
- Ignores pseudoconstant restriction clauses as they won't provide useful matches
- Part of PostgreSQL's partition-aware query optimization infrastructure
- Enables consistent handling of boolean vs. non-boolean partition key columns in pathkey generation
- Returns true if a matching boolean restriction clause is found, false otherwise