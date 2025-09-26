# SampleScan

## Location
src/include/nodes/plannodes.h: 405 - 410

## Overview
SampleScan represents a table sample scan plan node that retrieves a statistical sample of tuples from a relation using various sampling methods.

## Definition


## Detailed Description
The SampleScan structure represents a sampling scan operation in PostgreSQL's query execution plan. It inherits from the abstract Scan base type and implements statistical sampling of table data using various sampling methods such as BERNOULLI (row-level sampling) or SYSTEM (block-level sampling). This scan type is used to implement the TABLESAMPLE clause in SQL queries, which allows users to retrieve a representative subset of data from large tables for analysis or testing purposes.

Sample scans are particularly useful for statistical analysis, query testing on large datasets, or when approximate results are acceptable and performance is more important than completeness.

## Parameters / Member Variables
- : The base Scan structure containing the Plan node and scanrelid that identifies which relation to sample
- : A pointer to the TableSampleClause structure that contains the sampling method specification, sample parameters, and any additional sampling arguments

## Dependencies
- Functions called/Symbols referenced:
  - Scan (inherited base structure)
  - TableSampleClause (sampling specification structure)

- Called from (representative examples):
  - ExecInitSampleScan (executor initialization for sample scans)
  - create_samplescan_plan (planner function to create sample scan plans)
  - make_samplescan (utility function to construct SampleScan nodes)
  - set_plan_refs (plan reference setting)
  - ExecInitNode (general node initialization)
  - ExplainNode (query explain functionality)

## Notes and Other Information
- Implements the SQL TABLESAMPLE clause functionality
- Supports different sampling methods like BERNOULLI (row-level) and SYSTEM (page-level) sampling
- The tablesample field uses a struct pointer to avoid circular header dependencies
- Sample percentage and other sampling parameters are stored in the TableSampleClause
- The actual execution logic is implemented in src/backend/executor/nodeSamplescan.c
- Sampling can significantly improve performance for analytical queries on large datasets
- Different sampling methods have different statistical properties and performance characteristics