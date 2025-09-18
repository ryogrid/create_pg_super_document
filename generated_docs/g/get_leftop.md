# get_leftop

## Location
src/include/nodes/nodeFuncs.h: 83 - 94

## Overview
A static inline utility function that extracts the left operand from a binary operator expression, or the only operand from a unary operator expression in PostgreSQL's parse tree.

## Definition
```c
static inline Node *
get_leftop(const void *clause)
```

## Detailed Description
This function provides access to the left (or first) argument of an OpExpr node in PostgreSQL's parse tree. For binary operators like '=', '<', '>', '+', etc., it returns the left operand. For unary operators like '-' (negation) or 'NOT', it returns the single operand. The function safely handles the case where the arguments list is empty by returning NULL. It uses the linitial() macro to access the first element of the arguments list, which is stored as a List structure in the OpExpr node.

## Parameters / Member Variables
- `clause`: A pointer to an OpExpr node (cast as void*). The caller is responsible for ensuring this is actually an OpExpr node.

## Dependencies
- Functions called/Symbols referenced:
  - OpExpr (node type)
  - linitial (macro to get first list element)
  - NIL (empty list constant)
- Called from (representative examples):
  - ExecIndexBuildScanKeys
  - MakeTidOpExpr
  - TidExprListCreate
  - addRangeClause
  - process_equivalence
  - match_clause_to_ordering_op
  - make_restrictinfo_internal
  - mergejoinscansel

## Notes and Other Information
- This is a static inline function defined in nodeFuncs.h, making it available to any file that includes this header
- The function assumes the input is a valid OpExpr node - no type checking is performed
- Returns NULL if the arguments list is empty (NIL)
- Commonly used in conjunction with get_rightop() to analyze binary operator expressions
- Critical for query optimization tasks like join planning, index selection, and selectivity estimation
- The returned Node* may need to be cast to the appropriate expression type depending on the context