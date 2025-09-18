# create_samplescan_plan

## Location
[src/backend/optimizer/plan/createplan.c:2955-3005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2955-L3005)

## Overview
Creates a sample scan plan node for scanning a base relation using table sampling with specified target list and restriction clauses.

## Definition
```c
static SampleScan *
create_samplescan_plan(PlannerInfo *root, Path *best_path,
                       List *tlist, List *scan_clauses)
```

## Detailed Description
The `create_samplescan_plan` function creates a `SampleScan` plan node that represents a table sampling operation on a base relation. Table sampling allows PostgreSQL to retrieve a random subset of rows from a table, which is useful for statistical analysis or quick data exploration without scanning the entire table. 

The function performs similar steps to `create_seqscan_plan` but additionally handles the `TableSampleClause` that specifies the sampling method and parameters. It validates that the relation has an associated table sample clause, processes the scan clauses, and handles parameterized paths by replacing variables in both the scan clauses and the table sample clause.

This function supports various sampling methods like BERNOULLI and SYSTEM, as specified in SQL standard table sampling syntax (e.g., `SELECT * FROM table TABLESAMPLE BERNOULLI(10)`).

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context information
- `best_path`: The chosen Path representing the sample scan, containing cost estimates and relation information
- `tlist`: Target list specifying which columns/expressions should be returned by the scan
- `scan_clauses`: List of RestrictInfo nodes representing WHERE clause conditions to be applied during the scan

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [make_samplescan](../m/make_samplescan.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - RTE_RELATION (enum value)
  - SampleScan (struct type)
  - [TableSampleClause](../T/TableSampleClause.md) (struct type)
- Called from (representative examples):
  - [create_scan_plan](create_scan_plan.md)

## Notes and Other Information
- This function is static and only used within the createplan.c module
- Includes assertion checks to ensure the path represents a valid base relation with a table sample clause
- Handles parameterization of both scan clauses and table sample clause expressions
- Table sampling is particularly useful for large tables where a representative sample is sufficient
- The sampling method and percentage are determined by the TableSampleClause parsed from the SQL query
- Unlike sequential scans, sample scans may not return the same rows on repeated executions due to their random nature