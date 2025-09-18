# make_seqscan

## Location
[src/backend/optimizer/plan/createplan.c:5509-5525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5509-L5525)

## Overview
A plan node building function that creates and initializes a SeqScan plan node for sequential table scanning operations.

## Definition
```c
static SeqScan *make_seqscan(List *qptlist, List *qpqual, Index scanrelid)
```

## Detailed Description
This function is part of PostgreSQL's plan node building infrastructure and creates a SeqScan plan node that represents a sequential scan operation on a table. The function allocates a new SeqScan node using makeNode(), initializes its basic Plan structure with the provided target list and qualification conditions, and sets up the scan-specific parameters. As per the general convention for plan node building routines, this function does not perform cost calculations - those are handled by the caller, typically by calling copy_generic_path_info() to transfer cost information from the corresponding Path node. The function creates a simple scan node with no child nodes (lefttree and righttree are set to NULL).

## Parameters / Member Variables
- `qptlist`: The target list specifying which columns to output from the scan
- `qpqual`: The qualification conditions (WHERE clause predicates) to apply during the scan
- `scanrelid`: The relation identifier (table ID) to be scanned

## Dependencies
- Functions called/Symbols referenced:
  - SeqScan (the plan node type being created)
  - makeNode (PostgreSQL's node allocation macro)
- Called from (representative examples):
  - [create_seqscan_plan](../c/create_seqscan_plan.md)

## Notes and Other Information
- This is a static function within createplan.c for internal module use
- Part of the plan node building infrastructure that separates node creation from cost calculation
- Creates nodes with no child plans (lefttree and righttree are NULL) since sequential scans are leaf nodes in the plan tree
- The caller is responsible for filling in cost and width information from the corresponding Path node
- Follows PostgreSQL's convention of not performing cost calculations within plan building functions
- The created SeqScan node will be used during execution to perform actual table scanning