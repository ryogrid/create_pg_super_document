# extract_strong_not_arg

## Location
[src/backend/optimizer/util/predtest.c:1414-1459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L1414-L1459)

## Overview
Extracts the argument from clauses that assert the definite falsity of a subclause, providing a stricter form of negation detection than extract_not_arg.

## Definition

```c
static Node *
extract_strong_not_arg(Node *clause)
```
## Detailed Description
This utility function is similar to extract_not_arg but implements a more restrictive definition of negation. While extract_not_arg recognizes any form of non-truth (including NULL/UNKNOWN states), extract_strong_not_arg only recognizes definite falsity.

The function identifies two forms of strong negation:
1. **Explicit NOT expressions**: Direct boolean NOT operations (NOT_EXPR) 
2. **IS FALSE tests**: Boolean tests that explicitly assert falsity

The key difference from extract_not_arg is that this function excludes IS_NOT_TRUE and IS_UNKNOWN boolean tests, as these can be true when the argument evaluates to NULL, whereas strong negation requires the argument to be definitively false.

This distinction is important in PostgreSQL's three-valued logic system where NULL values create different logical semantics than simple true/false evaluations.

## Parameters / Member Variables
- : The expression node to examine for strong negation patterns, may be NULL

## Dependencies
- Functions called/Symbols referenced:
  - IsA (for node type checking)
  - linitial (for accessing first list element)
  - BoolExpr (boolean expression node type)
  - BooleanTest (boolean test node type)
  - NOT_EXPR (boolean NOT operation type)
  - IS_FALSE (boolean test type for definite falsity)
- Called from (representative examples):
  - iterate_end
  - [predicate_refuted_by_recurse](../p/predicate_refuted_by_recurse.md)

## Notes and Other Information
- Returns NULL if the input clause is NULL or doesn't match strong negation patterns
- More restrictive than extract_not_arg - only handles definite falsity, not general non-truth
- Excludes IS_NOT_TRUE and IS_UNKNOWN tests which can be satisfied by NULL values
- Used in contexts where three-valued logic distinctions matter for correctness
- Assumes well-formed NOT expressions with exactly one argument (accessed via linitial)