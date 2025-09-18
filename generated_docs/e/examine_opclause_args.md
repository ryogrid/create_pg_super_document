# examine_opclause_args

## Location
src/backend/statistics/extended_stats.c: 2035 - 2089

## Overview
Parses and extracts the components of an operator expression by splitting the arguments into expression and constant parts, handling RelabelType wrapper nodes.

## Definition


## Detailed Description
This utility function analyzes operator clause arguments to identify expressions that match the pattern (Expr op Const) or (Const op Expr). It handles RelabelType wrapper nodes by stripping them from both sides of the expression before analysis. The function is essential for extended statistics processing as it separates the variable expression part from the constant value part of operator clauses, which is necessary for statistics matching and selectivity estimation. The function returns true if the arguments match the expected pattern, false otherwise.

## Parameters / Member Variables
- : List containing exactly two argument nodes from an operator expression
- : Output parameter - pointer to store the extracted expression node (can be NULL if not needed)
- : Output parameter - pointer to store the extracted constant node (can be NULL if not needed) 
- : Output parameter - boolean flag indicating whether the expression was found on the left side (true) or right side (false) of the operator (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - lsecond
  - RelabelType
  - list_length
  - linitial
  - IsA
- Called from (representative examples):
  - [statext_is_compatible_clause_internal](../s/statext_is_compatible_clause_internal.md)
  - mcv_get_match_bitmap

## Notes and Other Information
The function enforces that exactly two arguments are provided (checked via Assert), which is expected for binary operator expressions. RelabelType nodes are automatically stripped as they represent type coercion operations that don't affect the underlying data values. The function supports flexible output by allowing any of the output parameters to be NULL if the caller doesn't need that particular information. This design makes it suitable for various use cases where only specific parts of the parsed expression are needed.