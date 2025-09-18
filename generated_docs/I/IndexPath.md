# IndexPath

## Location
[src/include/nodes/pathnodes.h:1709-1719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1709-L1719)

## Overview
IndexPath represents an index scan execution path over a single index, supporting both regular index scans and index-only scans with detailed index usage specifications and cost information.

## Definition


## Detailed Description
IndexPath is a specialized Path structure that represents index scan operations over a single index. It extends the base Path structure with index-specific information needed for both regular index scans (T_IndexScan) and index-only scans (T_IndexOnlyScan), with the path.pathtype field indicating which type is intended.

The structure contains comprehensive information about how the index will be used. The indexclauses field contains a list of IndexClause nodes representing index-checkable restrictions with implicit AND semantics - an empty list indicates a full index scan. For ordered indexes, indexorderbys can contain ORDER BY expressions that are usable as ordering operators, with each expression guaranteed to have the index key on the left side of the operator.

The indexorderbycols field provides a corresponding list of integer column numbers (zero-based) matching the indexorderbys list, indicating which index column each ORDER BY expression should be used with. The indexscandir field specifies the scan direction - ForwardScanDirection for forward scans, BackwardScanDirection for backward scans of ordered indexes, with unordered indexes always using ForwardScanDirection.

Cost information is preserved in indextotalcost and indexselectivity to avoid recomputation when considering the same index for bitmap index/heap scans, while the Path's own costs represent IndexScan or IndexOnlyScan plan costs.

## Parameters / Member Variables
- : Base Path structure containing common path information
- : Pointer to IndexOptInfo describing the index to be scanned
- : List of IndexClause nodes representing index-checkable restrictions (empty for full scan)
- : List of ORDER BY expressions usable as ordering operators (NIL if none)
- : Integer list of index column numbers corresponding to indexorderbys
- : ScanDirection enum (ForwardScanDirection or BackwardScanDirection)
- : Cost of the index access operation
- : Selectivity estimate for the index conditions

## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base path structure)
  - [IndexOptInfo](IndexOptInfo.md) (index metadata and statistics)
  - [List](../L/List.md) (PostgreSQL's list structure)
  - ScanDirection (scan direction enumeration)
  - Cost (cost estimation type)
  - Selectivity (selectivity estimation type)

- Called from (representative examples):
  - [cost_index](../c/cost_index.md) (src/backend/optimizer/path/costsize.c:549)
  - [get_index_paths](../g/get_index_paths.md) (src/backend/optimizer/path/indxpath.c:742)
  - [build_index_paths](../b/build_index_paths.md) (src/backend/optimizer/path/indxpath.c:811)
  - [create_indexscan_plan](../c/create_indexscan_plan.md) (src/backend/optimizer/plan/createplan.c:3007)
  - [create_index_path](../c/create_index_path.md) (src/backend/optimizer/util/pathnode.c:1005)

## Notes and Other Information
- Used for both regular index scans and index-only scans (differentiated by path.pathtype)
- Empty indexclauses list indicates a full index scan
- ORDER BY expressions in indexorderbys must have index keys on the left side of operators
- Unordered indexes always use ForwardScanDirection
- Cost information is cached to avoid recomputation in bitmap scan considerations
- The structure supports complex index usage patterns including ordering and filtering
- Extensively used in index access method cost estimation functions
- Can be reparameterized for different join contexts