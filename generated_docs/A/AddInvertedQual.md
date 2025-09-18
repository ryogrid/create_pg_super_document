# AddInvertedQual

## Location
src/backend/rewrite/rewriteManip.c: 1125 - 1148

## Overview
AddInvertedQual inverts a given qualification expression and adds it to the WHERE clause of a query tree, ensuring proper NULL handling by using "IS NOT TRUE" semantics rather than simple negation.

## Definition
```c
void AddInvertedQual(Query *parsetree, Node *qual)
```

## Detailed Description
This function is part of PostgreSQL's rewrite system and is responsible for adding the logical inverse of a qualification expression to a query's WHERE clause. The key aspect of this function is that it implements proper three-valued logic inversion by creating a BooleanTest node with IS_NOT_TRUE semantics instead of simple NOT operation. This distinction is crucial for correct NULL handling - when the original qualification evaluates to NULL, "x IS NOT TRUE" evaluates to TRUE, while "NOT x" would evaluate to NULL.

The function creates a BooleanTest node wrapping the input qualification and delegates the actual addition to the WHERE clause to the AddQual function. If the input qualification is NULL, the function returns early without making any changes.

## Parameters / Member Variables
- `parsetree`: The Query structure to which the inverted qualification will be added
- `qual`: The Node representing the qualification expression to be inverted and added

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create BooleanTest node)
  - AddQual (to add the inverted qualification to the query)
- Symbols used:
  - BooleanTest (node type for boolean tests)
  - IS_NOT_TRUE (boolean test type constant)
- Called from (representative examples):
  - CopyAndAddInvertedQual (in rewriteHandler.c)
  - Functions that need to add negated conditions with proper NULL semantics

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:1125-1148
- The function specifically uses IS_NOT_TRUE rather than simple negation to handle NULL values correctly in three-valued logic
- The input qualification is not copied since AddQual will handle necessary copying
- The location field is set to -1, indicating no specific source location for the generated BooleanTest node
- This function is part of PostgreSQL's rule system and query rewriting infrastructure