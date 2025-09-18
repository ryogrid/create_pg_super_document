# is_notclause

## Location
[src/include/nodes/nodeFuncs.h:125-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/nodeFuncs.h#L125-L133)

## Overview
Tests whether a given clause is a NOT clause (BoolExpr with NOT_EXPR operator).

## Definition
```c
static inline bool is_notclause(const void *clause)
```

## Detailed Description
This function is a type-checking utility that determines whether a given clause represents a NOT expression. It performs a series of checks to ensure the clause is not NULL, is of type BoolExpr, and specifically has the NOT_EXPR boolean operator. This is commonly used throughout the PostgreSQL optimizer and planner to identify NOT clauses for special handling, negation processing, and logical optimization.

## Parameters / Member Variables
- `clause`: A pointer to the clause to be tested; expected to be a Node structure but passed as void* for generality

## Dependencies
- Functions called/Symbols referenced:
  - BoolExpr (structure type)
  - NOT_EXPR (enum value)
  - IsA (macro for type checking)
- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)
  - [match_boolean_index_clause](../m/match_boolean_index_clause.md)
  - [predicate_implied_by_simple_clause](../p/predicate_implied_by_simple_clause.md)
  - [dependency_is_compatible_clause](../d/dependency_is_compatible_clause.md)
  - mcv_get_match_bitmap

## Notes and Other Information
- This is an inline function defined in a header file for performance
- Part of a family of clause-testing functions that help categorize different types of boolean expressions
- The function safely handles NULL input by checking for it explicitly
- Used in query optimization for recognizing negation patterns and applying appropriate transformations
- Important for boolean index matching and partition pruning logic