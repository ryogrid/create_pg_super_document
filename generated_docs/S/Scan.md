# Scan

## Location
[src/include/nodes/plannodes.h:384-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L384-L390)

## Overview
Scan is an abstract base type that all relation scan plan types inherit from in PostgreSQL's query execution plan tree.

## Definition

```c
typedef struct Scan
{
	pg_node_attr(abstract)

	Plan		plan;
	Index		scanrelid;		/* relid is index into the range table */
} Scan;
```
## Detailed Description
The Scan struct serves as the foundational structure for all scan operations in PostgreSQL's query planner and executor. It is marked as abstract, meaning it is never instantiated directly but provides common functionality for all specific scan types like sequential scans, index scans, and bitmap scans. This inheritance-based design allows the executor and planner to handle different scan types uniformly while providing specialized behavior for each scan method.

The Scan structure extends the basic Plan node with scan-specific information, primarily the relation identifier that indicates which table or relation is being scanned.

## Parameters / Member Variables
- : The base Plan node containing common plan information such as target list, qualification conditions, and cost estimates
- : An index into the query's range table that identifies which relation (table) this scan operation targets

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (inherited base structure)
  - Index (type for scanrelid)

- Called from (representative examples):
  - [SeqScan](SeqScan.md) (sequential scan implementation)
  - [IndexScan](../I/IndexScan.md) (index scan implementation) 
  - [IndexOnlyScan](../I/IndexOnlyScan.md) (index-only scan implementation)
  - [SampleScan](SampleScan.md) (sample scan implementation)
  - [BitmapHeapScan](../B/BitmapHeapScan.md) (bitmap heap scan implementation)
  - [ExecScanFetch](../E/ExecScanFetch.md) (executor scan functions)
  - [create_indexscan_plan](../c/create_indexscan_plan.md) (planner functions)

## Notes and Other Information
- This is an abstract type marked with pg_node_attr(abstract), meaning it cannot be instantiated directly
- All scan node types in PostgreSQL inherit from this structure, providing a uniform interface for scan operations
- The scanrelid field is crucial for the executor to know which table to scan from the query's range table
- Located in plannodes.h, indicating this is part of the query planning infrastructure rather than the execution layer