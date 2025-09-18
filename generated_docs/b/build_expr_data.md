# build_expr_data

## Location
[src/backend/statistics/extended_stats.c:2250-2274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L2250-L2274)

## Overview
Constructs an array of AnlExprData structures for a list of expressions, preparing them for statistical analysis during extended statistics computation.

## Definition


## Detailed Description
This function creates and initializes the data structures needed to analyze expressions for extended statistics. For each expression in the input list, it creates an AnlExprData entry that contains the expression node and an associated VacAttrStats structure obtained by calling examine_expression. The function allocates memory for all expression data entries at once and populates them sequentially. Since this function operates on standalone expressions rather than table columns, some fields in the VacAttrStats structures may be artificially populated by examine_expression to work with the standard statistics infrastructure.

## Parameters / Member Variables
- : List of Node pointers representing the expressions to be analyzed
- : Target number of statistics buckets/samples, controlling the detail level of statistics collection

## Dependencies
- Functions called/Symbols referenced:
  - [AnlExprData](../A/AnlExprData.md)
  - [examine_expression](../e/examine_expression.md)
  - list_length
  - [palloc0](../p/palloc0.md)
  - lfirst
- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md)

## Notes and Other Information
The function uses palloc0 to allocate zero-initialized memory for the AnlExprData array, ensuring all fields start in a clean state. The stattarget parameter is passed through to examine_expression where it controls the granularity of statistics collection - higher values result in more detailed histograms and statistics. The resulting AnlExprData array is used by subsequent functions like compute_expr_stats to evaluate expressions against sample data and generate the actual statistical summaries.