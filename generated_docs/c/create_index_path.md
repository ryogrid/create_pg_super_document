# create_index_path

## Location
[src/backend/optimizer/util/pathnode.c:993-1041](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L993-L1041)

## Overview
Creates an IndexPath node representing an index scan or index-only scan access method, supporting both forward and backward scans with optional ordering and filtering clauses.

## Definition

```c
IndexPath *
create_index_path(PlannerInfo *root,
				  IndexOptInfo *index,
				  List *indexclauses,
				  List *indexorderbys,
				  List *indexorderbycols,
				  List *pathkeys,
				  ScanDirection indexscandir,
				  bool indexonly,
				  Relids required_outer,
				  double loop_count,
				  bool partial_path)
```
## Detailed Description
The  function constructs an IndexPath node for index-based access methods. It handles both regular index scans (which read both index and heap pages) and index-only scans (which read only index pages when all needed columns are available in the index). The function supports complex index operations including filtering clauses, ordering operators, and both forward and backward scan directions.

Key features of the created path:
- Sets pathtype to T_IndexScan or T_IndexOnlyScan based on indexonly parameter
- Stores detailed index information including clauses, ordering, and scan direction
- Supports parameterization for nested loop joins
- Handles both filtering (indexclauses) and ordering (indexorderbys) operations
- Maintains pathkeys to indicate the result ordering
- Always non-parallel at the path level (parallel handling occurs at higher levels)

The function delegates cost calculation to , which considers factors like index selectivity, heap access patterns, caching effects, and the loop_count for nested operations.

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning context and information
- : IndexOptInfo structure describing the index to be used for scanning
- : List of IndexClause nodes representing filtering conditions enforced during the scan
- : List of bare expressions used as index ordering operators for result ordering
- : Integer list of index column numbers (zero-based) that can be used with ordering operators
- : List describing the sort ordering provided by this path
- : Scan direction (ForwardScanDirection or BackwardScanDirection)
- : Boolean flag indicating whether to perform an index-only scan
- : Set of outer relation IDs needed for parameterized paths
- : Number of repetitions expected for caching behavior estimation
- : Boolean indicating if this is for parallel index scan construction

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new IndexPath node)
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md) (handles parameterization)
  - [cost_index](cost_index.md) (calculates index scan costs)
  - [IndexOptInfo](../I/IndexOptInfo.md), ScanDirection, IndexPath (types)
  - T_IndexScan, T_IndexOnlyScan (pathtype constants)

- Called from (representative examples):
  - [build_index_paths](../b/build_index_paths.md)
  - [plan_cluster_use_sort](../p/plan_cluster_use_sort.md)

## Notes and Other Information
- Returns an IndexPath pointer (subclass of Path) rather than a generic Path
- The indexonly parameter determines whether heap pages need to be accessed
- Index-only scans are generally cheaper but require all needed columns to be present in the index
- The pathkeys parameter reflects the ordering that can be provided by the index scan
- The loop_count parameter helps estimate caching benefits when the scan is repeated
- Parallel awareness is handled at higher levels; individual IndexPath nodes are not parallel-aware
- The function supports complex index operations like distance ordering for spatial indexes through indexorderbys