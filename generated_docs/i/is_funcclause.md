# is_funcclause

## Location
[src/include/nodes/nodeFuncs.h:69-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/nodeFuncs.h#L69-L75)

## Overview
A static inline utility function that checks whether a given clause is a FuncExpr (function expression) node in PostgreSQL's parse tree.

## Definition

```c
static inline bool
is_funcclause(const void *clause)
```
## Detailed Description
This function provides a type-safe way to determine if a clause represents a function call expression in PostgreSQL's parse tree. It uses the IsA() macro to perform runtime type checking, ensuring that the provided clause is not NULL and is specifically a FuncExpr node type. This is commonly used in optimizer and planner code where different types of clauses need to be handled differently based on their node type.

## Parameters / Member Variables
- : A pointer to a parse tree node that needs to be checked. Can be NULL, in which case the function returns false.

## Dependencies
- Functions called/Symbols referenced:
  - [FuncExpr](../F/FuncExpr.md) (node type)
  - IsA (macro for type checking)
- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)
  - [clause_is_strict_for](../c/clause_is_strict_for.md)
  - [array_unnest_support](../a/array_unnest_support.md)
  - [generate_series_int4_support](../g/generate_series_int4_support.md)
  - [generate_series_int8_support](../g/generate_series_int8_support.md)
  - [like_regex_support](../l/like_regex_support.md)
  - [network_subset_support](../n/network_subset_support.md)

## Notes and Other Information
- This is a static inline function defined in nodeFuncs.h, making it available to any file that includes this header
- The function is NULL-safe, returning false when clause is NULL
- Part of a family of similar type-checking functions for different node types in PostgreSQL's parse tree
- Commonly used in selectivity estimation, predicate testing, and function support routines

## Simplified Source

```c
static inline bool
is_funcclause(const void *clause)
{
    // Check if clause is non-NULL and is a FuncExpr node
    return clause != NULL && IsA(clause, FuncExpr);
}
```