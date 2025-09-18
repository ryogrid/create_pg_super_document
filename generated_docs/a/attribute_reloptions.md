# attribute_reloptions

## Location
[src/backend/access/common/reloptions.c:2078-2094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L2078-L2094)

## Overview
Parses and validates relation options specifically for table attributes (columns), handling statistics-related options that influence query planning.

## Definition


## Detailed Description
The `attribute_reloptions` function is a specialized option parser for PostgreSQL table attributes (columns) that processes column-specific options used by the query planner for optimization. It defines two key options: `n_distinct` and `n_distinct_inherited`, both of which are real-valued parameters that provide hints to the query planner about the number of distinct values in a column. These statistics override the planner's automatic estimates and can significantly impact query plan selection and performance. The function uses the standard `build_reloptions` infrastructure with RELOPT_KIND_ATTRIBUTE to ensure consistent parsing and validation of attribute-level options.

## Parameters / Member Variables
- `reloptions`: Datum containing the raw relation options to be parsed and processed
- `validate`: Boolean flag indicating whether to perform validation of the option values during parsing

## Dependencies
- Functions called/Symbols referenced:
  - [build_reloptions](../b/build_reloptions.md)
  - relopt_parse_elt (structure)
  - RELOPT_TYPE_REAL (constant)
  - RELOPT_KIND_ATTRIBUTE (constant)
  - AttributeOpts (structure)
  - lengthof (macro)
- Called from (representative examples):
  - [ATExecSetOptions](../A/ATExecSetOptions.md)
  - get_attribute_options

## Notes and Other Information
- The n_distinct option allows manual specification of the estimated number of distinct values in a column, overriding automatic statistics
- The n_distinct_inherited option provides distinct value estimates for inheritance hierarchies where child tables are included in queries
- Both options accept real numbers where positive values represent absolute distinct counts and negative values represent fractions of total rows
- These options are primarily used for performance tuning when the automatic statistics collection doesn't accurately capture column cardinality
- Attribute options are stored per-column and can be set using ALTER TABLE ... ALTER COLUMN ... SET STATISTICS or similar commands
- The options directly influence the PostgreSQL cost-based optimizer's decision-making process for join order, index usage, and other query plan choices