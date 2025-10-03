# makeDependencyGraph

## Location
[src/backend/parser/parse_cte.c:648-669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_cte.c#L648-L669)

## Overview
Identifies cross-references between WITH RECURSIVE items and sorts them into an order that eliminates forward references using topological sorting.

## Definition

```c
static void
makeDependencyGraph(CteState *cstate)
```
## Detailed Description
This function analyzes the dependencies between CTEs in a recursive WITH clause to determine a safe processing order. It performs a two-step process:

1. **Dependency Analysis**: For each CTE, it walks through the CTE's query tree using makeDependencyGraphWalker to identify which other CTEs it references. This builds up the dependency relationships in the CteState structure.

2. **Topological Sorting**: After all dependencies are identified, it calls TopologicalSort to arrange the CTEs in an order where each CTE is processed only after all the CTEs it depends on have been processed.

This ordering is crucial for recursive WITH clauses because it ensures that when a CTE is being analyzed, all the CTEs it references have already been analyzed and their types determined.

## Parameters / Member Variables
- `*cstate`: CteState structure containing the array of CTE items and dependency tracking information
## Dependencies
- Functions called/Symbols referenced:
  - [makeDependencyGraphWalker](makeDependencyGraphWalker.md) - walks through CTE query trees to find dependencies
  - [TopologicalSort](../T/TopologicalSort.md) - sorts CTE items based on their dependencies
- Called from (representative examples):
  - [transformWithClause](../t/transformWithClause.md) - called when processing recursive WITH clauses to determine processing order

## Notes and Other Information
- The function is static and only used within parse_cte.c
- Sets cstate->curitem to track which CTE is currently being analyzed
- Maintains cstate->innerwiths to track nested WITH clauses during analysis
- The Assert ensures that innerwiths is properly cleaned up after each CTE analysis
- Essential for preventing forward reference errors in recursive WITH clauses
- The topological sort will detect and report circular dependencies between CTEs

## Simplified Source

```c
static void
makeDependencyGraph(CteState *cstate)
{
    int i;

    // Analyze dependencies for each CTE
    for (i = 0; i < cstate->numitems; i++) {
        CommonTableExpr *cte = cstate->items[i].cte;

        cstate->curitem = i;
        cstate->innerwiths = NIL;
        makeDependencyGraphWalker((Node *) cte->ctequery, cstate);
        Assert(cstate->innerwiths == NIL);
    }

    // Sort CTEs based on dependencies using topological sort
    TopologicalSort(cstate->pstate, cstate->items, cstate->numitems);
}
```