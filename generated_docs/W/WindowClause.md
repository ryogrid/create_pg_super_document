# WindowClause

## Location
[src/include/nodes/parsenodes.h:1536-1562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1536-L1562)

## Overview
WindowClause represents the transformed representation of WINDOW and OVER clauses, providing the complete specification for window function processing including partitioning, ordering, and framing information.

## Definition

```c
typedef struct WindowClause
{
	NodeTag		type;
	/* window name (NULL in an OVER clause) */
	char	   *name pg_node_attr(query_jumble_ignore);
	/* referenced window name, if any */
	char	   *refname pg_node_attr(query_jumble_ignore);
	List	   *partitionClause;	/* PARTITION BY list */
	/* ORDER BY list */
	List	   *orderClause;
	int			frameOptions;	/* frame_clause options, see WindowDef */
	Node	   *startOffset;	/* expression for starting bound, if any */
	Node	   *endOffset;		/* expression for ending bound, if any */
	/* in_range function for startOffset */
	Oid			startInRangeFunc pg_node_attr(query_jumble_ignore);
	/* in_range function for endOffset */
	Oid			endInRangeFunc pg_node_attr(query_jumble_ignore);
	/* collation for in_range tests */
	Oid			inRangeColl pg_node_attr(query_jumble_ignore);
	/* use ASC sort order for in_range tests? */
	bool		inRangeAsc pg_node_attr(query_jumble_ignore);
	/* nulls sort first for in_range tests? */
	bool		inRangeNullsFirst pg_node_attr(query_jumble_ignore);
	Index		winref;			/* ID referenced by window functions */
	/* did we copy orderClause from refname? */
	bool		copiedOrder pg_node_attr(query_jumble_ignore);
} WindowClause;
```
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
  - [SortGroupClause](../S/SortGroupClause.md) (for partition and order specifications)
  - [Node](../N/Node.md) (for offset expressions)
  - [List](../L/List.md) (for clause storage)
- Called from (representative examples):
  - [transformWindowDefinitions](../t/transformWindowDefinitions.md) (parser/parse_clause.c)
  - [create_windowagg_plan](../c/create_windowagg_plan.md) (optimizer/plan/createplan.c)
  - [optimize_window_clauses](../o/optimize_window_clauses.md) (optimizer/plan/planner.c)
  - [make_pathkeys_for_window](../m/make_pathkeys_for_window.md) (optimizer/plan/planner.c)

## Notes and Other Information
- Window inheritance follows SQL standard rules: partition clauses always copied, order clauses may be copied, frame options never copied
- The winref field must be unique among all windows in a query's windowClause list
- [Query](../Q/Query.md) planner sanitizes partitionClause to remove redundant PathKeys for optimization
- RANGE frame semantics with offsets require special in_range functions and collation handling
- Window clause optimization can merge or reorder windows to minimize sorting overhead
- Multiple WindowFunc nodes can reference the same WindowClause via winref