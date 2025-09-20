# RangeQueryClause

## Location
[src/backend/optimizer/path/clausesel.c:31-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/clausesel.c#L31-L39)

## Overview
A data structure used for accumulating information about possible range-query clause pairs during selectivity estimation in PostgreSQL's query optimizer.

## Definition

```c
typedef struct RangeQueryClause
{
	struct RangeQueryClause *next;	/* next in linked list */
	Node	   *var;			/* The common variable of the clauses */
	bool		have_lobound;	/* found a low-bound clause yet? */
	bool		have_hibound;	/* found a high-bound clause yet? */
	Selectivity lobound;		/* Selectivity of a var > something clause */
	Selectivity hibound;		/* Selectivity of a var < something clause */
} RangeQueryClause;
```
## Detailed Description
The RangeQueryClause structure is used by PostgreSQL's query optimizer to identify and optimize range queries (conditions like ) during selectivity estimation. When the optimizer encounters multiple clauses that potentially form range conditions on the same variable, it uses this structure to group them together and calculate more accurate selectivity estimates.

The structure maintains information about both lower and upper bounds for a given variable, allowing the optimizer to recognize when clauses can be combined into a single range condition rather than treating them as independent predicates. This optimization is crucial for accurate cardinality estimation, which directly impacts query plan quality.

The structure is organized as a linked list, where each node represents a different variable that has potential range conditions. For each variable, the structure tracks whether lower and/or upper bounds have been found, along with their respective selectivity values.

## Parameters / Member Variables
- `*next`: Pointer to the next RangeQueryClause in the linked list, enabling multiple variables to be tracked simultaneously
- `*var`: Node representing the common variable referenced by the range clauses (e.g., a column reference)
- `have_lobound`: Boolean flag indicating whether a lower-bound clause (var > something) has been identified for this variable
- `have_hibound`: Boolean flag indicating whether an upper-bound clause (var < something) has been identified for this variable
- `lobound`: Selectivity estimate for the lower-bound clause, representing the fraction of rows expected to satisfy the condition
- `hibound`: Selectivity estimate for the upper-bound clause, representing the fraction of rows expected to satisfy the condition
## Dependencies
- Functions called/Symbols referenced:
  - [Node](../N/Node.md) (PostgreSQL's base node type)
  - Selectivity (PostgreSQL's selectivity type)
- Called from (representative examples):
  - [clauselist_selectivity_ext](../c/clauselist_selectivity_ext.md) (src/backend/optimizer/path/clausesel.c:127, 271)
  - [addRangeClause](../a/addRangeClause.md) (src/backend/optimizer/path/clausesel.c:427, 430, 498)

## Notes and Other Information
- The structure is defined in src/backend/optimizer/path/clausesel.c:31-39
- Used internally by the query optimizer for selectivity estimation and is not exposed to user code
- The selectivity values stored represent the fraction of rows expected to satisfy each bound condition
- When both bounds are present, the optimizer can calculate a more accurate combined selectivity for the range condition
- The addRangeClause function handles the logic for identifying matching variables and updating bound information
- Part of PostgreSQL's cost-based optimization system that helps choose efficient query execution plans