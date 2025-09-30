# make_indexscan

## Location
[src/backend/optimizer/plan/createplan.c:5545-5575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5545-L5575)

## Overview
A plan node building function that creates and initializes an IndexScan plan node for index-based table access operations.

## Definition
```c
static IndexScan *make_indexscan(List *qptlist, List *qpqual, Index scanrelid, Oid indexid, List *indexqual, List *indexqualorig, List *indexorderby, List *indexorderbyorig, List *indexorderbyops, ScanDirection indexscandir)
```

## Detailed Description
This function is part of PostgreSQL's plan node building infrastructure and creates an IndexScan plan node that represents an index-based access to table data. Index scans use an index structure to efficiently locate and retrieve specific rows from a table, typically much faster than sequential scans for selective queries. The function sets up all the necessary parameters for index scanning including the index to use, qualification conditions that can be evaluated using the index, ordering requirements, and scan direction. Like other plan building functions, it focuses on node structure initialization and leaves cost calculations to the caller. The function handles both simple index lookups and more complex scenarios involving ordering and original qualification preservation.

## Parameters / Member Variables
- `qptlist`: The target list specifying which columns to output from the scan
- `qpqual`: The qualification conditions (WHERE clause predicates) to apply after index lookup
- `scanrelid`: The relation identifier (table ID) to be scanned
- `indexid`: The OID of the index to be used for scanning
- `indexqual`: The index qualification conditions (conditions that can be evaluated using the index)
- `indexqualorig`: The original form of index qualifications before any transformations
- `indexorderby`: The ordering expressions for index-based sorting
- `indexorderbyorig`: The original form of ordering expressions before transformations
- `indexorderbyops`: The operator OIDs for index ordering operations
- `indexscandir`: The scan direction (forward, backward, or no movement)

## Dependencies
- Functions called/Symbols referenced:
  - [IndexScan](../I/IndexScan.md) (the plan node type being created)
  - ScanDirection (enumeration for scan direction)
  - makeNode (PostgreSQL's node allocation macro)
- Called from (representative examples):
  - [create_indexscan_plan](../c/create_indexscan_plan.md)

## Notes and Other Information
- This is a static function within createplan.c for internal module use
- Part of the plan node building infrastructure that separates node creation from cost calculation
- Creates nodes with no child plans (lefttree and righttree are NULL) since index scans are leaf nodes
- Handles complex index access patterns including ordered retrieval and multiple qualification levels
- The distinction between indexqual and qpqual allows for optimal query execution where some conditions are evaluated via index and others after row retrieval
- Supports various index types and scan directions for flexible query execution
- The original forms of qualifications and orderings are preserved for debugging and plan analysis purposes

## Simplified Source

```c
static IndexScan *make_indexscan(List *qptlist, List *qpqual, Index scanrelid,
                                Oid indexid, List *indexqual, List *indexqualorig,
                                List *indexorderby, List *indexorderbyorig,
                                List *indexorderbyops, ScanDirection indexscandir) {
    // Create new IndexScan node
    IndexScan *node = makeNode(IndexScan);
    Plan *plan = &node->scan.plan;

    // Set basic plan properties
    plan->targetlist = qptlist;  // Output columns
    plan->qual = qpqual;         // Post-index filter conditions
    plan->lefttree = NULL;       // No child plans (leaf node)
    plan->righttree = NULL;

    // Configure index scan specifics
    node->scan.scanrelid = scanrelid;        // Table to scan
    node->indexid = indexid;                 // Index to use
    node->indexqual = indexqual;             // Index-based conditions
    node->indexqualorig = indexqualorig;     // Original conditions
    node->indexorderby = indexorderby;       // Index ordering
    node->indexorderbyorig = indexorderbyorig;
    node->indexorderbyops = indexorderbyops;
    node->indexorderdir = indexscandir;      // Scan direction

    return node;
}
```