# pull_ands

## Location
[src/backend/optimizer/prep/prepqual.c:323-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepqual.c#L323-L348)

## Overview
Recursively flattens nested AND clauses into a single flat list of AND arguments, eliminating unnecessary nesting in boolean expressions.

## Definition

```c
static List *
pull_ands(List *andlist)
```
## Detailed Description
The  function takes the argument list of an AND clause and recursively flattens any nested AND clauses within it. This transformation converts deeply nested AND structures like  into a flat list  that can be processed more efficiently.

The function iterates through each argument in the input AND list. When it encounters a nested AND clause (detected using ), it recursively calls itself to flatten that subclause and concatenates the results. Non-AND expressions are simply appended to the output list as-is.

This flattening is important for query optimization as it:
- Simplifies the structure for further processing
- Eliminates unnecessary intermediate nodes
- Makes it easier to apply other optimizations like duplicate removal
- Reduces the depth of expression trees

The original input list structure is preserved (not modified), and a new flattened list is returned.

## Parameters / Member Variables
- : The argument list of an AND clause to be flattened (List of Node pointers)

## Dependencies
- Functions called/Symbols referenced:
  -  - checks if a node is an AND boolean expression
  -  - concatenates two lists together
  -  - recursive call to handle nested AND clauses
  -  - boolean expression node type for accessing args
- Called from (representative examples):
  -  (recursive call) (src/backend/optimizer/prep/prepqual.c:334)
  -  (src/backend/optimizer/prep/prepqual.c:491)
  -  (src/backend/optimizer/prep/prepqual.c:675)

## Notes and Other Information
- This is a static function, only used within the prepqual.c module
- The function preserves the original input list structure and returns a new list
- Essential for boolean expression normalization in PostgreSQL's query optimizer
- Works recursively to handle arbitrarily deep nesting of AND clauses
- Part of the boolean expression flattening system that also includes  for OR clauses
- The flattening helps subsequent optimization passes work more effectively on simplified structures