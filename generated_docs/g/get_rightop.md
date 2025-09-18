# get_rightop

## Location
src/include/nodes/nodeFuncs.h: 95 - 106

## Overview
A static inline utility function that extracts the right operand from a binary operator expression in PostgreSQL's parse tree, returning NULL for unary operator expressions.

## Definition
```c
static inline Node *
get_rightop(const void *clause)
```

## Detailed Description
This function provides access to the right (or second) argument of an OpExpr node in PostgreSQL's parse tree. For binary operators like '=', '<', '>', '+', etc., it returns the right operand. For unary operators, it returns NULL since there is no second operand. The function checks that the arguments list has at least 2 elements before attempting to access the second element using the lsecond() macro. This ensures safe access to binary operator arguments while gracefully handling unary operators.

## Parameters / Member Variables
- `clause`: A pointer to an OpExpr node (cast as void*). The caller is responsible for ensuring this is actually an OpExpr node.

## Dependencies
- Functions called/Symbols referenced:
  - OpExpr (node type)
  - lsecond (macro to get second list element)
  - list_length (function to get list length)
- Called from (representative examples):
  - [ExecIndexBuildScanKeys](../E/ExecIndexBuildScanKeys.md)
  - [MakeTidOpExpr](../M/MakeTidOpExpr.md)
  - [TidExprListCreate](../T/TidExprListCreate.md)
  - [addRangeClause](../a/addRangeClause.md)
  - [process_equivalence](../p/process_equivalence.md)
  - [match_clause_to_ordering_op](../m/match_clause_to_ordering_op.md)
  - [make_restrictinfo_internal](../m/make_restrictinfo_internal.md)
  - [mergejoinscansel](../m/mergejoinscansel.md)

## Notes and Other Information
- This is a static inline function defined in nodeFuncs.h, making it available to any file that includes this header
- The function assumes the input is a valid OpExpr node - no type checking is performed
- Returns NULL if the arguments list has fewer than 2 elements (unary operators)
- Commonly used in conjunction with get_leftop() to analyze binary operator expressions
- Critical for query optimization tasks like join planning, index selection, and selectivity estimation
- The returned Node* may need to be cast to the appropriate expression type depending on the context
- Used extensively in the same contexts as get_leftop(), often called together to extract both operands of binary expressions