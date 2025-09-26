# is_opclause

## Location
[src/include/nodes/nodeFuncs.h:76-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/nodeFuncs.h#L76-L82)

## Overview
A static inline utility function that checks whether a given clause is an OpExpr (operator expression) node in PostgreSQL's parse tree.

## Definition
```c
static inline bool
is_opclause(const void *clause)
```

## Detailed Description
This function provides a type-safe way to determine if a clause represents an operator expression in PostgreSQL's parse tree. It uses the IsA() macro to perform runtime type checking, ensuring that the provided clause is not NULL and is specifically an OpExpr node type. OpExpr nodes represent binary and unary operator expressions like comparisons (=, <, >), arithmetic operations (+, -, *, /), and other operators. This function is extensively used throughout the optimizer, planner, and executor components where different handling is required based on whether a clause is an operator expression.

## Parameters / Member Variables
- `clause`: A pointer to a parse tree node that needs to be checked. Can be NULL, in which case the function returns false.

## Dependencies
- Functions called/Symbols referenced:
  - [OpExpr](../O/OpExpr.md) (node type)
  - IsA (macro for type checking)
- Called from (representative examples):
  - [TidExprListCreate](../T/TidExprListCreate.md)
  - [clauselist_selectivity_ext](../c/clauselist_selectivity_ext.md)
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)
  - [process_equivalence](../p/process_equivalence.md)
  - [match_clause_to_ordering_op](../m/match_clause_to_ordering_op.md)
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md)
  - [CommuteOpExpr](../C/CommuteOpExpr.md)
  - [make_restrictinfo_internal](../m/make_restrictinfo_internal.md)
  - [mergejoinscansel](../m/mergejoinscansel.md)

## Notes and Other Information
- This is a static inline function defined in nodeFuncs.h, making it available to any file that includes this header
- The function is NULL-safe, returning false when clause is NULL
- Part of a family of similar type-checking functions for different node types in PostgreSQL's parse tree
- Heavily used in join planning, selectivity estimation, predicate testing, and statistics collection
- [OpExpr](../O/OpExpr.md) is one of the most common expression types in SQL queries, making this function critical for query optimization