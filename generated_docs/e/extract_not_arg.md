# extract_not_arg

## Location
[src/backend/optimizer/util/predtest.c:1386-1413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L1386-L1413)

## Overview
Extracts the argument from clauses that assert the non-truth of a subclause, used in PostgreSQL's predicate testing for handling negated expressions.

## Definition


## Detailed Description
This utility function identifies clauses that express negation and extracts the negated subexpression. It recognizes two main forms of negation in PostgreSQL's expression tree:

1. **Explicit NOT expressions**: Direct boolean NOT operations (NOT_EXPR)
2. **Boolean test negations**: Boolean tests that assert non-truth such as IS NOT TRUE, IS FALSE, or IS UNKNOWN

The function is essential for predicate testing logic where the optimizer needs to work with both positive and negative forms of conditions. By extracting the core argument from negated expressions, other predicate testing functions can apply transformation rules and logical equivalences.

## Parameters / Member Variables
- : The expression node to examine for negation patterns, may be NULL

## Dependencies
- Functions called/Symbols referenced:
  - IsA (for node type checking)
  - linitial (for accessing first list element)
  - BoolExpr (boolean expression node type)
  - BooleanTest (boolean test node type)  
  - NOT_EXPR (boolean NOT operation type)
  - IS_NOT_TRUE, IS_FALSE, IS_UNKNOWN (boolean test types)
- Called from (representative examples):
  - iterate_end
  - [predicate_refuted_by_recurse](../p/predicate_refuted_by_recurse.md) (multiple call sites)

## Notes and Other Information
- Returns NULL if the input clause is NULL or doesn't match any negation pattern
- Only extracts from single-argument NOT expressions (using linitial)
- Treats IS_NOT_TRUE, IS_FALSE, and IS_UNKNOWN boolean tests as equivalent forms of negation
- Does not handle IS_NOT_FALSE or IS_TRUE as these represent affirmative conditions
- The function assumes well-formed expression trees where NOT expressions have exactly one argument