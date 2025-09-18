# CteState

## Location
src/backend/parser/parse_cte.c: 71 - 83

## Overview
CteState is a comprehensive state structure used during CTE (Common Table Expression) parsing and analysis to maintain global context and working state information across tree walker functions.

## Definition
```c
typedef struct CteState
{
	/* global state: */
	ParseState *pstate;			/* global parse state */
	CteItem    *items;			/* array of CTEs and extra data */
	int			numitems;		/* number of CTEs */
	/* working state during a tree walk: */
	int			curitem;		/* index of item currently being examined */
	List	   *innerwiths;		/* list of lists of CommonTableExpr */
	/* working state for checkWellFormedRecursion walk only: */
	int			selfrefcount;	/* number of self-references detected */
	RecursionContext context;	/* context to allow or disallow self-ref */
} CteState;
```

## Detailed Description
CteState serves as a central coordination structure during CTE processing, particularly for WITH RECURSIVE clauses. It maintains both global state information that persists throughout the entire CTE analysis process and temporary working state that changes as tree walker functions traverse the query structure.

The structure supports multiple phases of CTE analysis: dependency graph construction, topological sorting, and recursive well-formedness checking. It enables the parser to track relationships between CTEs, detect recursive references, and ensure that recursive CTEs conform to SQL standard requirements. The state is passed between various walker functions that analyze different aspects of CTE structure and semantics.

## Parameters / Member Variables
- `pstate`: Pointer to the global ParseState, providing access to the overall parsing context and utilities
- `items`: Array of CteItem structures representing all CTEs being analyzed, with their dependency information
- `numitems`: Number of CteItem entries in the items array
- `curitem`: Index of the CteItem currently being examined during tree traversal (working state)
- `innerwiths`: List of lists containing CommonTableExpr nodes for nested WITH clauses encountered during processing
- `selfrefcount`: Counter tracking the number of self-references detected during recursive well-formedness checking
- `context`: Current RecursionContext indicating whether self-references are allowed in the current parsing context

## Dependencies
- Functions called/Symbols referenced:
  - [ParseState](../P/ParseState.md)
  - [CteItem](CteItem.md)
  - [RecursionContext](../R/RecursionContext.md)
  - [List](../L/List.md)
  - CommonTableExpr
- Called from (representative examples):
  - [transformWithClause](../t/transformWithClause.md)
  - [makeDependencyGraph](../m/makeDependencyGraph.md)
  - [makeDependencyGraphWalker](../m/makeDependencyGraphWalker.md)
  - [checkWellFormedRecursion](../c/checkWellFormedRecursion.md)
  - [checkWellFormedRecursionWalker](../c/checkWellFormedRecursionWalker.md)

## Notes and Other Information
- The structure is designed to be passed through multiple phases of CTE analysis, with different fields being relevant during different phases
- The `innerwiths` field handles the complex case of nested WITH clauses within recursive CTEs
- The recursive well-formedness checking fields (`selfrefcount` and `context`) are specifically used to enforce SQL standard restrictions on where recursive references can appear
- This state management approach allows PostgreSQL to handle complex recursive CTE scenarios while maintaining proper error reporting and validation