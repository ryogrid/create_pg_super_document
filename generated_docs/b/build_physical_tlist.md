# build_physical_tlist

## Location
[src/backend/optimizer/util/plancat.c:1764-1884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1764-L1884)

## Overview
Constructs a target list consisting of exactly the relation's user attributes in order, enabling executor optimizations by avoiding projection steps at runtime.

## Definition

```c
List *
build_physical_tlist(PlannerInfo *root, RelOptInfo *rel)
```
## Detailed Description
This function builds a "physical" target list that matches the actual physical layout of a relation's columns. The executor can special-case such target lists to avoid projection operations, providing significant performance benefits for scan nodes. The function handles multiple types of range table entries including base relations, subqueries, functions, values lists, CTEs, and other table expressions.

The function creates a target list where each entry corresponds to a relation attribute in its natural order. However, it applies a conservative approach: if any dropped columns or columns with missing values are detected, it returns NIL (empty list) to punt the optimization. This avoids complications with type information that may no longer be available for dropped columns.

For different RTE kinds:
- **RTE_RELATION**: Iterates through relation attributes, creating Var nodes for each
- **RTE_SUBQUERY**: Maps subquery target list entries to Var nodes
- **RTE_FUNCTION/TABLEFUNC/VALUES/CTE/etc**: Uses expandRTE to get column information

## Parameters / Member Variables
- : PlannerInfo containing global planner state and range table information
- : RelOptInfo representing the relation for which to build the physical target list

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch (retrieves range table entry by index)
  - [table_open](../t/table_open.md)/table_close (relation access functions)
  - RelationGetNumberOfAttributes (gets attribute count)
  - [makeVar](../m/makeVar.md) (creates Var nodes for table columns)
  - [makeTargetEntry](../m/makeTargetEntry.md) (creates target list entries)
  - [makeVarFromTargetEntry](../m/makeVarFromTargetEntry.md) (creates Var from subquery target entry)
  - [expandRTE](../e/expandRTE.md) (expands range table entry to column list)
  - TupleDescAttr (accesses tuple descriptor attributes)
  - RTE_RELATION, RTE_SUBQUERY, RTE_FUNCTION, etc. (range table entry kinds)

- Called from (representative examples):
  - [create_scan_plan](../c/create_scan_plan.md) (src/backend/optimizer/plan/createplan.c:659)

## Notes and Other Information
- Returns NIL when dropped columns (attisdropped) or missing columns (atthasmissing) are encountered
- Supports optimization for various scan node types: SeqScan, SubqueryScan, FunctionScan, ValuesScan, CteScan, etc.
- Critical for avoiding unnecessary projection overhead in the executor
- The optimization is especially valuable for wide tables where projection costs would be significant
- Conservative approach ensures type safety by avoiding issues with dropped column types
- Location: src/backend/optimizer/util/plancat.c:1764-1884