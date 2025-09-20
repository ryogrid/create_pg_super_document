# set_indexonlyscan_references

## Location
[src/backend/optimizer/plan/setrefs.c:1321-1394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L1321-L1394)

## Overview
Specialized function that adjusts variable references in IndexOnlyScan plan nodes, converting heap-referencing variables to index-referencing variables for index-only scan operations.

## Definition

```c
static Plan *
set_indexonlyscan_references(PlannerInfo *root,
							 IndexOnlyScan *plan,
							 int rtoffset)
```
## Detailed Description
 handles the unique requirements of IndexOnlyScan nodes, which differ significantly from regular IndexScan nodes. The key challenge is that IndexOnlyScan nodes must convert variables that originally referenced heap columns into variables that reference corresponding index columns, since the scan will only access the index without touching the heap.

The function performs several critical operations:
1. Creates a "stripped" index targetlist by removing non-returnable columns (marked as resjunk)
2. Builds an indexed_tlist structure from the returnable index columns
3. Uses  to transform targetlist, qual, and recheckqual expressions to reference index columns via INDEX_VAR
4. Processes indexqual and indexorderby expressions (which already reference index columns) with standard reference fixing
5. Handles the indextlist specially, as it must NOT be transformed to reference index columns

The distinction between expressions that get converted to INDEX_VAR (targetlist, qual, recheckqual) and those that don't (indexqual, indexorderby, indextlist) is crucial for correct execution.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state and context information
- : The IndexOnlyScan node to process and adjust variable references for
- : Integer offset to add to rangetable indices for proper variable resolution

## Dependencies
- Functions called/Symbols referenced:
  - [build_tlist_index](../b/build_tlist_index.md): Creates indexed lookup structure from targetlist
  - [fix_upper_expr](../f/fix_upper_expr.md): Transforms expressions to use different variable sources (INDEX_VAR)
  - fix_scan_list: Standard variable reference adjustment for scan expressions
  - INDEX_VAR: Special variable type for referencing index columns
  - NRM_EQUAL: Name resolution mode for expression fixing
  - NUM_EXEC_TLIST/NUM_EXEC_QUAL: Macros for determining execution context
- Called from (representative examples):
  - [set_plan_refs](set_plan_refs.md): When processing IndexOnlyScan nodes in the main plan tree traversal
  - fix_scan_list: During recursive plan reference adjustment

## Notes and Other Information
IndexOnlyScan is the only scan type that requires this specialized handling because it's the only scan that changes the source of column values from heap tuples to index tuples. The function carefully distinguishes between expressions that need INDEX_VAR transformation (those that will be evaluated using index column values) and those that don't (those that are already in index column terms or serve structural purposes). The recheckqual field is particularly important as it contains conditions that must be rechecked after retrieving heap tuples if the index scan's visibility map indicates potential tuple visibility issues.