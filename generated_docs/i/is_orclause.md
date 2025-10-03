# is_orclause

## Location
[src/include/nodes/nodeFuncs.h:116-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/nodeFuncs.h#L116-L124)

## Overview
Tests whether a given clause is an OR clause (BoolExpr with OR_EXPR operator).

## Definition

```c
static inline bool
is_orclause(const void *clause)
```
## Detailed Description
This function is a type-checking utility that determines whether a given clause represents an OR expression. It performs a series of checks to ensure the clause is not NULL, is of type BoolExpr, and specifically has the OR_EXPR boolean operator. This is commonly used throughout the PostgreSQL optimizer and planner to identify OR clauses for special handling, optimization, and transformation.

## Parameters / Member Variables
- `*clause`: A pointer to the clause to be tested; expected to be a Node structure but passed as void* for generality
## Dependencies
- Functions called/Symbols referenced:
  - [BoolExpr](../B/BoolExpr.md) (structure type)
  - OR_EXPR (enum value)
  - IsA (macro for type checking)
- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)
  - [pull_ors](../p/pull_ors.md)
  - [extract_or_clause](../e/extract_or_clause.md)
  - [simplify_or_arguments](../s/simplify_or_arguments.md)
  - [predicate_classify](../p/predicate_classify.md)

## Notes and Other Information
- This is an inline function defined in a header file for performance
- Part of a family of clause-testing functions that help categorize different types of boolean expressions
- The function safely handles NULL input by checking for it explicitly
- Widely used throughout the optimizer for OR clause detection and special processing

## Simplified Source

```c
static inline bool is_orclause(const void *clause)
{
    // Check if clause is non-NULL BoolExpr with OR_EXPR operator
    return (clause != NULL &&
            IsA(clause, BoolExpr) &&
            ((const BoolExpr *) clause)->boolop == OR_EXPR);
}
```