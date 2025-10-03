# make_indexonlyscan

## Location
[src/backend/optimizer/plan/createplan.c:5576-5604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5576-L5604)

## Overview
Creates and initializes an IndexOnlyScan plan node for the PostgreSQL query planner, which represents an index-only scan operation that can return results directly from index pages without accessing the heap table.

## Definition

```c
static IndexOnlyScan *
make_indexonlyscan(List *qptlist,
				   List *qpqual,
				   Index scanrelid,
				   Oid indexid,
				   List *indexqual,
				   List *recheckqual,
				   List *indexorderby,
				   List *indextlist,
				   ScanDirection indexscandir)
```
## Detailed Description
This function constructs an IndexOnlyScan plan node, which is an optimization that allows PostgreSQL to satisfy queries entirely from index data without needing to access the underlying heap table. This is particularly efficient when the index contains all the columns needed to answer the query. The function initializes all necessary fields of the IndexOnlyScan structure including scan qualifications, ordering specifications, and the target list that can be computed directly from the index.

## Parameters / Member Variables
- `*qptlist`: Target list of expressions to be computed for this plan node
- `*qpqual`: Qualification conditions that apply to this scan operation
- `scanrelid`: Range table index of the relation being scanned
- `indexid`: Object identifier of the index to be used for the scan
- `*indexqual`: Index-specific qualification conditions that can be applied during index traversal
- `*recheckqual`: Conditions that need to be rechecked after retrieving tuples from the index
- `*indexorderby`: List of expressions for ordering the index scan results
- `*indextlist`: Target list of expressions that can be computed directly from index columns
- `indexscandir`: Direction for scanning the index (forward, backward, or no movement)
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the IndexOnlyScan node)
- Called from (representative examples):
  - [create_indexscan_plan](../c/create_indexscan_plan.md)

## Notes and Other Information
- This is a static function within createplan.c, indicating it's used internally for plan creation
- Index-only scans are a significant performance optimization as they avoid heap access
- The function sets up both the basic Plan fields and IndexOnlyScan-specific fields
- The recheck qualification is important for handling cases where index conditions may not be fully reliable

## Simplified Source

```c
static IndexOnlyScan *make_indexonlyscan(List *qptlist, List *qpqual, Index scanrelid,
                                          Oid indexid, List *indexqual, List *recheckqual,
                                          List *indexorderby, List *indextlist,
                                          ScanDirection indexscandir) {
    // Create new IndexOnlyScan node
    IndexOnlyScan *node = makeNode(IndexOnlyScan);
    Plan *plan = &node->scan.plan;

    // Set up basic plan fields
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;   // Leaf scan node
    plan->righttree = NULL;  // Leaf scan node

    // Set IndexOnlyScan specific fields
    node->scan.scanrelid = scanrelid;       // Relation being scanned
    node->indexid = indexid;                // Index to use
    node->indexqual = indexqual;            // Index qualifications
    node->recheckqual = recheckqual;        // Conditions to recheck
    node->indexorderby = indexorderby;      // Ordering expressions
    node->indextlist = indextlist;          // Target list from index
    node->indexorderdir = indexscandir;     // Scan direction

    return node;
}
```