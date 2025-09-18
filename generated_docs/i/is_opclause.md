# is_opclause

## Location
src/include/nodes/nodeFuncs.h: 76 - 82

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
  - OpExpr (node type)
  - IsA (macro for type checking)
- Called from (representative examples):
  - TidExprListCreate
  - clauselist_selectivity_ext
  - clause_selectivity_ext
  - process_equivalence
  - match_clause_to_ordering_op
  - create_hashjoin_plan
  - CommuteOpExpr
  - make_restrictinfo_internal
  - mergejoinscansel

## Notes and Other Information
- This is a static inline function defined in nodeFuncs.h, making it available to any file that includes this header
- The function is NULL-safe, returning false when clause is NULL
- Part of a family of similar type-checking functions for different node types in PostgreSQL's parse tree
- Heavily used in join planning, selectivity estimation, predicate testing, and statistics collection
- OpExpr is one of the most common expression types in SQL queries, making this function critical for query optimization