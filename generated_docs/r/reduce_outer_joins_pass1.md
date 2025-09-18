# reduce_outer_joins_pass1

## Location
src/backend/optimizer/prep/prepjointree.c: 3006 - 3083

## Overview
Phase 1 data collection function that recursively traverses the jointree to gather information about base relations and outer joins for the outer join reduction optimization.

## Definition
```c
static reduce_outer_joins_pass1_state *reduce_outer_joins_pass1(Node *jtnode)
```

## Detailed Description
This function performs the first pass of the outer join reduction algorithm by recursively walking the jointree and collecting essential information needed for optimization decisions in pass 2. It builds a comprehensive state structure that tracks:

1. **Relation Identification**: Collects all base relation IDs (relids) that appear below each jointree node
2. **Outer Join Detection**: Identifies whether outer joins exist anywhere below the current node
3. **Hierarchical Structure**: Maintains sub-state information for child nodes to enable efficient pass 2 processing

The function handles three main types of jointree nodes:
- **RangeTblRef**: Leaf nodes representing base relations - adds the relation ID to the result
- **FromExpr**: FROM clause expressions with multiple items - recursively processes each item in the fromlist
- **JoinExpr**: Join expressions - processes left and right arguments, marking when outer joins are encountered

This information gathering phase is crucial for the optimizations efficiency, allowing pass 2 to avoid unnecessary strictness checks by knowing exactly where outer joins exist and which relations are involved.

## Parameters / Member Variables
- `jtnode`: The jointree node to analyze (can be RangeTblRef, FromExpr, JoinExpr, or NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - [bms_add_members](../b/bms_add_members.md)
  - lappend
  - IS_OUTER_JOIN
  - nodeTag
  - elog
- Called from (representative examples):
  - [reduce_outer_joins](reduce_outer_joins.md) (initial call)
  - [reduce_outer_joins_pass1](reduce_outer_joins_pass1.md) (recursive calls)

## Notes and Other Information
- Returns a reduce_outer_joins_pass1_state structure containing collected information
- The function is static (internal to prepjointree.c) and only used within the outer join reduction algorithm
- Handles NULL jointree nodes gracefully by returning an empty state
- The recursive nature allows it to build a complete picture of the jointree structure
- Join expressions own RT indexes are intentionally excluded from the relids result
- Error handling is provided for unrecognized node types to aid debugging