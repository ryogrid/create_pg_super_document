# PredIterInfoData

## Location
[src/backend/optimizer/util/predtest.c:59-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L59-L70)

## Overview
PredIterInfoData is the core structure that implements a generic iteration framework for traversing different types of expression nodes in PostgreSQL's predicate testing system during query optimization.

## Definition

```c
typedef struct PredIterInfoData
{
	/* node-type-specific iteration state */
	void	   *state;
	List	   *state_list;
	/* initialize to do the iteration */
	void		(*startup_fn) (Node *clause, PredIterInfo info);
	/* next-component iteration function */
	Node	   *(*next_fn) (PredIterInfo info);
	/* release resources when done with iteration */
	void		(*cleanup_fn) (PredIterInfo info);
} PredIterInfoData;
```
## Detailed Description
PredIterInfoData implements a generic iterator pattern for traversing various expression node types during predicate analysis in PostgreSQL's optimizer. This structure provides a unified interface for iterating over different expression formats such as Lists (implicit AND), BoolExpr nodes (explicit AND/OR), and ScalarArrayOpExpr nodes. The framework is essential for the predicate testing logic that determines logical relationships between WHERE clause conditions.

The structure uses function pointers to provide node-type-specific behavior while maintaining a common iteration interface. This allows the predicate testing algorithms to uniformly process different expression structures without needing to know their internal representation. The iteration state is maintained in the state and state_list fields, which are interpreted differently depending on the node type being processed.

## Parameters / Member Variables
- `*state`: Generic pointer to node-type-specific iteration state (e.g., current ListCell position for Lists)
- `*state_list`: Pointer to the List being iterated over (used by List and BoolExpr iterators)
- `info)`: Function pointer to initialize the iteration for a specific clause type
- `info)`: Function pointer that returns the next component node in the iteration sequence
- `info)`: Function pointer to release any resources allocated during iteration
## Dependencies
- Functions called/Symbols referenced:
  - [PredIterInfo](PredIterInfo.md) (typedef for pointer to this structure)
  - [Node](../N/Node.md) (base type for expression nodes)
  - [List](../L/List.md) (PostgreSQL's list structure)
- Called from (representative examples):
  - [predicate_implied_by_recurse](../p/predicate_implied_by_recurse.md)
  - [predicate_refuted_by_recurse](../p/predicate_refuted_by_recurse.md)
  - [predicate_classify](../p/predicate_classify.md)

## Notes and Other Information
- The framework supports three main iterator implementations: list_*_fn for Lists, boolexpr_startup_fn + list_*_fn for BoolExpr, and arrayconst_*_fn/arrayexpr_*_fn for array operations
- This design pattern enables efficient traversal of complex logical expressions during query optimization
- The structure is always used as a local variable on the stack, never dynamically allocated
- Essential component of PostgreSQL's logical inference system that helps optimize queries by reasoning about predicate relationships
- The cleanup function is often a no-op for simple cases but important for array expression handling that may allocate temporary resources