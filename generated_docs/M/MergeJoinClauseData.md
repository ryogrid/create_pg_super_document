# MergeJoinClauseData

## Location
[src/backend/executor/nodeMergejoin.c:120-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L120-L140)

## Overview
MergeJoinClauseData is a runtime data structure that encapsulates all the information needed to execute a single merge join clause, including expression states, cached values, and comparison functions.

## Definition

```c
typedef struct MergeJoinClauseData
{
	/* Executable expression trees */
	ExprState  *lexpr;			/* left-hand (outer) input expression */
	ExprState  *rexpr;			/* right-hand (inner) input expression */

	/*
	 * If we have a current left or right input tuple, the values of the
	 * expressions are loaded into these fields:
	 */
	Datum		ldatum;			/* current left-hand value */
	Datum		rdatum;			/* current right-hand value */
	bool		lisnull;		/* and their isnull flags */
	bool		risnull;

	/*
	 * Everything we need to know to compare the left and right values is
	 * stored here.
	 */
	SortSupportData ssup;
}			MergeJoinClauseData;
```
## Detailed Description
MergeJoinClauseData represents the runtime state for a single merge join clause during execution. Each instance corresponds to one equality condition in the merge join's join quals. The structure is designed to optimize performance by:

1. **Pre-compiled expressions**: The lexpr and rexpr fields contain pre-compiled expression states that can be efficiently evaluated during join execution.

2. **Cached evaluation results**: The ldatum/rdatum and lisnull/risnull fields cache the most recently computed values from the expressions, avoiding redundant evaluations when the same tuple is compared multiple times.

3. **Optimized comparison**: The SortSupportData provides fast comparison functions that are set up once during initialization and used repeatedly during join processing.

The structure is created and initialized by MJExamineQuals() during merge join node initialization, with one MergeJoinClauseData instance created for each mergejoinable condition provided by the planner.

## Parameters / Member Variables
- `*lexpr`: Pre-compiled expression state for evaluating the left-hand (outer relation) side of the join condition
- `*rexpr`: Pre-compiled expression state for evaluating the right-hand (inner relation) side of the join condition
- `ldatum`: Cached result value from the most recent evaluation of lexpr
- `rdatum`: Cached result value from the most recent evaluation of rexpr
- `lisnull`: NULL indicator flag for ldatum
- `risnull`: NULL indicator flag for rdatum
- `ssup`: Sort support data containing comparison functions, collation info, sort direction, and null handling rules for efficiently comparing ldatum and rdatum
## Dependencies
- Functions called/Symbols referenced:
  - [SortSupportData](../S/SortSupportData.md)
  - [ExprState](../E/ExprState.md)
  - Datum
- Called from (representative examples):
  - [MJExamineQuals](MJExamineQuals.md) (creates and initializes arrays of MergeJoinClauseData)
  - [MergeJoinClause](MergeJoinClause.md) (typedef pointer to this structure)

## Notes and Other Information
- This structure is private to nodeMergejoin.c and is not exposed in header files
- The structure is designed for high-performance tuple-by-tuple comparison during merge join execution
- The SortSupportData member provides access to optimized comparison routines that can handle different data types, collations, and sort orders
- Memory for arrays of these structures is allocated in the query's per-tuple memory context during node initialization
- The cached datum values are updated as the merge join algorithm advances through the sorted input streams