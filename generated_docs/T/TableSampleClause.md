# TableSampleClause

## Location
src/include/nodes/parsenodes.h: 1344 - 1350

## Overview
TableSampleClause represents a TABLESAMPLE clause that appears in a transformed FROM clause, providing table sampling functionality for query execution.

## Definition


## Detailed Description
TableSampleClause is a subnode of RangeTblEntry that represents table sampling functionality in PostgreSQL. Unlike RangeTableSample (used in raw parse trees), TableSampleClause appears in transformed query trees after parsing and analysis are complete.

Table sampling allows queries to work with a subset of table data, which is useful for statistical analysis, performance optimization on large datasets, and approximate query processing. The clause specifies a sampling method handler function, arguments to control the sampling behavior, and an optional REPEATABLE expression for deterministic sampling results.

The tsmhandler field references a tablesample method handler function (like SYSTEM or BERNOULLI sampling methods), while the args list contains expressions that parameterize the sampling behavior (such as sample percentage). The repeatable expression, when provided, ensures that multiple executions of the same query with the same REPEATABLE value produce identical sampling results.

## Parameters / Member Variables
- : NodeTag identifying this as a TableSampleClause node
- : OID of the tablesample method handler function (e.g., SYSTEM, BERNOULLI)
- : List of expressions providing arguments to the sampling method (e.g., sample percentage)
- : Optional REPEATABLE expression for deterministic sampling; NULL if not specified

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - Oid
  - [List](../L/List.md)
  - [Expr](../E/Expr.md)
- Called from (representative examples):
  - [transformRangeTableSample](../t/transformRangeTableSample.md)
  - [ExecInitSampleScan](../E/ExecInitSampleScan.md)
  - [create_samplescan_plan](../c/create_samplescan_plan.md)
  - [cost_samplescan](../c/cost_samplescan.md)
  - [set_tablesample_rel_size](../s/set_tablesample_rel_size.md)
  - [show_tablesample](../s/show_tablesample.md)
  - get_tablesample_def

## Notes and Other Information
- Appears as a subnode of RangeTblEntry after query transformation
- Distinct from RangeTableSample which is used in raw parse trees
- Supports various sampling methods through pluggable handler functions
- The REPEATABLE clause enables reproducible sampling results across query executions
- Used by the planner to estimate costs and create appropriate execution plans
- Integrated with PostgreSQL's extensible tablesample method framework
- Critical for implementing efficient statistical queries on large datasets
- Arguments are evaluated at execution time, allowing dynamic sampling parameters