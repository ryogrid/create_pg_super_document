# make_samplescan

## Location
src/backend/optimizer/plan/createplan.c: 5526 - 5544

## Overview
A plan node building function that creates and initializes a SampleScan plan node for table sampling operations using TABLESAMPLE clauses.

## Definition
```c
static SampleScan *make_samplescan(List *qptlist, List *qpqual, Index scanrelid, TableSampleClause *tsc)
```

## Detailed Description
This function is part of PostgreSQL's plan node building infrastructure and creates a SampleScan plan node that represents a table sampling operation. Table sampling allows PostgreSQL to read only a statistical sample of rows from a table rather than the entire table, which is useful for approximate query results or performance optimization on large datasets. The function allocates a new SampleScan node, initializes its basic Plan structure with the provided target list and qualification conditions, sets the relation to scan, and most importantly, attaches the TableSampleClause that defines the sampling method and parameters. Like other plan building functions, this does not perform cost calculations, leaving that responsibility to the caller.

## Parameters / Member Variables
- `qptlist`: The target list specifying which columns to output from the scan
- `qpqual`: The qualification conditions (WHERE clause predicates) to apply during the scan
- `scanrelid`: The relation identifier (table ID) to be sampled
- `tsc`: The TableSampleClause containing the sampling method and parameters

## Dependencies
- Functions called/Symbols referenced:
  - SampleScan (the plan node type being created)
  - TableSampleClause (structure defining sampling parameters)
  - makeNode (PostgreSQL's node allocation macro)
- Called from (representative examples):
  - create_samplescan_plan

## Notes and Other Information
- This is a static function within createplan.c for internal module use
- Part of the plan node building infrastructure that separates node creation from cost calculation
- Creates nodes with no child plans (lefttree and righttree are NULL) since sample scans are leaf nodes
- The TableSampleClause parameter is critical as it contains the specific sampling method (e.g., SYSTEM, BERNOULLI) and parameters
- Supports PostgreSQL's TABLESAMPLE functionality introduced in SQL standard for statistical sampling
- The caller is responsible for filling in cost and width information from the corresponding Path node
- Used for implementing queries like 'SELECT * FROM table TABLESAMPLE SYSTEM (10)'