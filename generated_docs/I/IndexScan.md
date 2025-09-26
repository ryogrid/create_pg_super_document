# IndexScan

## Location
[src/include/nodes/plannodes.h:449-459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L449-L459)

## Overview
IndexScan represents an index scan plan node that retrieves tuples from a relation by traversing one or more indexes to locate qualifying rows efficiently.

## Definition

```c
typedef struct IndexScan
{
	Scan		scan;
	Oid			indexid;		/* OID of index to scan */
	List	   *indexqual;		/* list of index quals (usually OpExprs) */
	List	   *indexqualorig;	/* the same in original form */
	List	   *indexorderby;	/* list of index ORDER BY exprs */
	List	   *indexorderbyorig;	/* the same in original form */
	List	   *indexorderbyops;	/* OIDs of sort ops for ORDER BY exprs */
	ScanDirection indexorderdir;	/* forward or backward or don't care */
} IndexScan;
```
## Detailed Description
The IndexScan structure represents an index-based scan operation in PostgreSQL's query execution plan. It inherits from the abstract Scan base type and provides efficient tuple retrieval by using index structures to locate qualifying rows without scanning the entire table. Index scans are typically more efficient than sequential scans when selecting a small percentage of rows or when specific sort orders are required.

The structure contains both original and transformed versions of index qualification expressions to support both planning-time optimization and runtime execution. It also supports ordered scans for queries with ORDER BY clauses that match the index ordering.

## Parameters / Member Variables
- : The base Scan structure containing the Plan node and scanrelid
- : The OID (object identifier) of the specific index to scan
- : List of index qualification expressions with index keys on the left side, used for execution
- : Original form of index qualification expressions as they appeared in the WHERE clause
- : List of ORDER BY expressions modified to have index column variables on the left
- : Original form of ORDER BY expressions, used for runtime rechecking and EXPLAIN
- : List of operator OIDs for ORDER BY expressions, used with indexorderbyorig for runtime recheck
- : Scan direction (forward, backward, or don't care) for ordered index scans

## Dependencies
- Functions called/Symbols referenced:
  - Scan (inherited base structure)
  - ScanDirection (enumeration for scan direction)
  - Oid (object identifier type)
  - List (PostgreSQL list type)

- Called from (representative examples):
  - ExecInitIndexScan (executor initialization for index scans)
  - IndexNext (main index scan execution function)
  - IndexNextWithReorder (index scan with result reordering)
  - make_indexscan (planner utility to create IndexScan nodes)
  - create_bitmap_subplan (bitmap scan creation)
  - ExplainNode (query explanation functionality)

## Notes and Other Information
- Index scans are typically more efficient than sequential scans for selective queries
- The dual representation (original and transformed) supports both optimization and execution needs
- Supports both equality and range scans depending on the index type and query conditions
- Can provide ordered results without additional sorting when query ORDER BY matches index order
- The indexorderdir field allows bidirectional scanning on ordered indexes
- Index scans can be combined with bitmap operations for complex query patterns
- Runtime recheck capabilities handle lossy index conditions and verify sort ordering
- The actual execution logic is implemented in src/backend/executor/nodeIndexscan.c