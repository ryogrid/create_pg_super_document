# make_recursive_union

## Location
src/backend/optimizer/plan/createplan.c: 5863 - 5918

## Overview
Creates and initializes a RecursiveUnion plan node that implements recursive Common Table Expressions (CTEs) by managing the union between base and recursive branches while detecting duplicates.

## Definition
```c
static RecursiveUnion *
make_recursive_union(List *tlist,
                    Plan *lefttree,
                    Plan *righttree,
                    int wtParam,
                    List *distinctList,
                    long numGroups)
```

## Detailed Description
This function constructs a RecursiveUnion plan node, which is the core operator for executing recursive CTEs. It manages the iterative evaluation of recursive queries by alternating between the base case (lefttree) and recursive case (righttree) until no new rows are produced. The function sets up duplicate detection mechanisms using equality operators and collations derived from the distinctList parameter. This ensures that the recursive process terminates when no new distinct rows are generated.

## Parameters / Member Variables
- `tlist`: Target list defining the output columns of the recursive union
- `lefttree`: Plan node for the base (non-recursive) part of the CTE  
- `righttree`: Plan node for the recursive part of the CTE
- `wtParam`: Parameter ID for the working table used to store intermediate results
- `distinctList`: List of SortGroupClause nodes defining columns used for duplicate detection
- `numGroups`: Estimated number of distinct groups (used for planning purposes)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate RecursiveUnion node)
  - list_length (to get distinctList size)
  - palloc (to allocate arrays for duplicate detection)
  - get_sortgroupclause_tle (to resolve target list entries)
  - exprCollation (to get expression collations)
  - RecursiveUnion (struct type)
  - SortGroupClause (struct type)
- Called from (representative examples):
  - create_recursiveunion_plan

## Notes and Other Information
- The function is static, meaning it's only accessible within the createplan.c file
- The duplicate detection mechanism is crucial for ensuring recursive CTE termination
- Arrays dupColIdx, dupOperators, and dupCollations are allocated and populated to enable efficient duplicate checking during execution
- The wtParam links this node to a specific working table that stores intermediate results between recursive iterations
- The function handles the case where no duplicate detection is needed (numCols = 0) gracefully
- The plan.qual is always set to NIL since qualification is handled by the child plans