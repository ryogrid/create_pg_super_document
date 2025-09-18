# is_andclause

## Location
[src/include/nodes/nodeFuncs.h:107-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/nodeFuncs.h#L107-L115)

## Overview
A static inline utility function that checks whether a given clause is an AND boolean expression node in PostgreSQL's parse tree.

## Definition
```c
static inline bool
is_andclause(const void *clause)
```

## Detailed Description
This function provides a type-safe way to determine if a clause represents an AND boolean expression in PostgreSQL's parse tree. It performs three checks: first ensuring the clause is not NULL, then verifying it's a BoolExpr node type using the IsA() macro, and finally checking that the specific boolean operation type is AND_EXPR. This is used extensively throughout the query planner and optimizer to identify AND clauses that can be decomposed, reordered, or optimized using various techniques like predicate pushdown, join elimination, and index selection.

## Parameters / Member Variables
- `clause`: A pointer to a parse tree node that needs to be checked. Can be NULL, in which case the function returns false.

## Dependencies
- Functions called/Symbols referenced:
  - BoolExpr (node type)
  - AND_EXPR (boolean operation constant)
  - IsA (macro for type checking)
- Called from (representative examples):
  - [ExecInitSubPlan](../E/ExecInitSubPlan.md)
  - [make_ands_implicit](../m/make_ands_implicit.md)
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)
  - [generate_bitmap_or_paths](../g/generate_bitmap_or_paths.md)
  - [pull_ands](../p/pull_ands.md)
  - [simplify_and_arguments](../s/simplify_and_arguments.md)
  - [extract_or_clause](../e/extract_or_clause.md)
  - [make_restrictinfo](../m/make_restrictinfo.md)
  - [gen_partprune_steps_internal](../g/gen_partprune_steps_internal.md)
  - mcv_get_match_bitmap

## Notes and Other Information
- This is a static inline function defined in nodeFuncs.h, making it available to any file that includes this header
- The function is NULL-safe, returning false when clause is NULL
- Part of a family of similar type-checking functions for different node types in PostgreSQL's parse tree
- Extensively used in query optimization, particularly for logical simplification and predicate manipulation
- AND clauses are fundamental to SQL query structure and optimization, making this function critical for many optimizer operations
- Often used in conjunction with clause decomposition routines that break down complex AND expressions into individual conjuncts
- Important for partition pruning, statistics collection, and join planning algorithms