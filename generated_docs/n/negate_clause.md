# negate_clause

## Location
[src/backend/optimizer/prep/prepqual.c:73-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepqual.c#L73-L292)

## Overview
Negates a Boolean expression by logical simplification, attempting to eliminate the NOT node through DeMorgan's laws and other boolean transformations rather than simply wrapping the expression in a NOT clause.

## Definition


## Detailed Description
The  function takes a Boolean expression and returns its logical negation, but does so intelligently by applying various logical simplification rules rather than just adding a NOT node. It is primarily designed as a helper function for  and preserves AND/OR flat structure in the input.

Key transformations applied:
- **Constants**: Negates boolean constants directly (true becomes false, false becomes true, NULL remains NULL)
- **Operators**: Uses negator operators when available (< becomes >=, = becomes <>, etc.)
- **ScalarArrayOpExpr**: Negates array operators and flips ANY/ALL semantics
- **BoolExpr**: Applies DeMorgan's laws to AND/OR expressions:
  - NOT(A AND B) becomes (NOT A) OR (NOT B)
  - NOT(A OR B) becomes (NOT A) AND (NOT B)
  - NOT(NOT A) becomes A (double negation elimination)
- **NullTest**: Flips IS NULL to IS NOT NULL and vice versa (scalar types only)
- **BooleanTest**: Flips various boolean test types (IS_TRUE to IS_NOT_TRUE, etc.)

The function unconditionally applies DeMorgan's laws even if it results in more NOT nodes, because exposing top-level AND/OR structure is crucial for WHERE clause optimization and ensuring logically equivalent expressions are physically equal.

## Parameters / Member Variables
- : The Boolean expression node to negate (should not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  -  - determines the node type
  -  - creates boolean constant nodes
  -  - finds the negator operator for a given operator
  -  - creates OR expression nodes
  -  - creates AND expression nodes
  -  - creates NOT expression nodes as fallback
- Called from (representative examples):
  -  (src/backend/optimizer/util/clauses.c:2905)
  -  (src/backend/optimizer/util/clauses.c:4006)
  -  (src/backend/partitioning/partprune.c:3736)
  - Recursively calls itself when processing AND/OR expressions

## Notes and Other Information
- The function preserves the AND/OR flat property of input expressions, which is important for query optimization
- For expressions that cannot be simplified, it falls back to wrapping with an explicit NOT node
- The transformation ensures that logically equivalent expressions will be physically equal after processing
- Handles special cases like double negation elimination and null handling appropriately
- Part of the PostgreSQL query optimizer's constant expression evaluation and boolean simplification system