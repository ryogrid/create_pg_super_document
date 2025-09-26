# SeqScan

## Location
src/include/nodes/plannodes.h: 396 - 399

## Overview
SeqScan represents a sequential scan plan node that performs a full table scan by reading every tuple in a relation sequentially.

## Definition

```c
typedef struct SeqScan
{
	Scan		scan;
} SeqScan;
```
## Detailed Description
The SeqScan structure represents a sequential scan operation in PostgreSQL's query execution plan. It inherits from the abstract Scan base type and implements the simplest form of table access by reading through all tuples in a relation from beginning to end. This scan method is used when no suitable indexes are available, when the query requires a large portion of the table's data, or when the optimizer determines that a sequential scan would be more efficient than an index scan.

Sequential scans are the most straightforward scan method but can be expensive for large tables. However, they have predictable I/O patterns and can be very efficient when most of the table needs to be read.

## Parameters / Member Variables
- : The base Scan structure containing the Plan node and scanrelid that identifies which relation to scan sequentially

## Dependencies
- Functions called/Symbols referenced:
  - Scan (inherited base structure)

- Called from (representative examples):
  - ExecInitSeqScan (executor initialization)
  - create_seqscan_plan (planner function to create sequential scan plans)
  - make_seqscan (utility function to construct SeqScan nodes)
  - set_plan_refs (plan reference setting)
  - ExecInitNode (general node initialization)

## Notes and Other Information
- This is the simplest and most basic scan type in PostgreSQL
- Sequential scans read every page of the relation in physical order
- Often used as a fallback when no indexes are available or when index scans would be less efficient
- The actual execution logic is implemented in src/backend/executor/nodeSeqscan.c
- Sequential scans benefit from read-ahead and can be parallelized in PostgreSQL
- The planner chooses sequential scans based on cost estimates comparing it to available index scans