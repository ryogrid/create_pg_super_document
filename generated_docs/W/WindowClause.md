# WindowClause

## Location
src/include/nodes/parsenodes.h: 1536 - 1562

## Overview
WindowClause represents the transformed representation of WINDOW and OVER clauses, providing the complete specification for window function processing including partitioning, ordering, and framing information.

## Definition


## Detailed Description
WindowClause contains the complete specification for window function processing after parsing and analysis. It supports both named windows (from WINDOW clauses) and inline window specifications (from OVER clauses), with duplicate OVER specifications being collapsed during processing.

The structure handles window inheritance where one window can reference another through refname. When inheritance occurs, the partition clause is always copied from the referenced window, the order clause may be copied (tracked by copiedOrder), but framing options are never inherited per SQL specification.

For RANGE frame specifications with offset bounds, the structure maintains detailed information about the in_range functions and collation requirements needed for proper boundary calculations. The query planner optimizes the partitionClause by removing columns that belong to redundant PathKeys.

## Parameters / Member Variables
- : NodeTag identifying this as a WindowClause node
- : Name of the window if originally from a WINDOW clause, NULL for OVER clauses
- : Name of referenced window for inheritance, if any
- : List of SortGroupClause nodes defining PARTITION BY specification
- : List of SortGroupClause nodes defining ORDER BY specification
- : Bit flags specifying frame clause options (see WindowDef)
- : Expression defining the starting frame boundary offset
- : Expression defining the ending frame boundary offset
- : OID of in_range function for start boundary calculations
- : OID of in_range function for end boundary calculations
- : Collation OID for in_range function calls
- : Boolean indicating ASC sort order for in_range tests
- : Boolean indicating null handling for in_range tests
- : Unique identifier referenced by WindowFunc nodes
- : Boolean indicating if orderClause was inherited from refname

## Dependencies
- Functions called/Symbols referenced:
  - SortGroupClause (for partition and order specifications)
  - Node (for offset expressions)
  - List (for clause storage)
- Called from (representative examples):
  - transformWindowDefinitions (parser/parse_clause.c)
  - create_windowagg_plan (optimizer/plan/createplan.c)
  - optimize_window_clauses (optimizer/plan/planner.c)
  - make_pathkeys_for_window (optimizer/plan/planner.c)

## Notes and Other Information
- Window inheritance follows SQL standard rules: partition clauses always copied, order clauses may be copied, frame options never copied
- The winref field must be unique among all windows in a query's windowClause list
- Query planner sanitizes partitionClause to remove redundant PathKeys for optimization
- RANGE frame semantics with offsets require special in_range functions and collation handling
- Window clause optimization can merge or reorder windows to minimize sorting overhead
- Multiple WindowFunc nodes can reference the same WindowClause via winref