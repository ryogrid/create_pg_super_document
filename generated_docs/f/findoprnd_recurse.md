# findoprnd_recurse

## Location
src/backend/utils/adt/tsquery.c: 726 - 783

## Overview
A recursive function that traverses a tsquery in polish notation to fill in left-offset fields for operators and detect stop words that need cleanup.

## Definition


## Detailed Description
The findoprnd_recurse function performs a recursive traversal of a tsquery structure stored in polish (prefix) notation to compute and fill in the left-offset fields for operators. It processes the query tree by examining each node type: for value nodes (QI_VAL) it simply advances position, for stop word nodes (QI_VALSTOP) it marks that cleanup is needed, and for operator nodes (QI_OPR) it recursively processes operands. For binary operators (AND, OR, PHRASE), it processes the right operand first, then calculates the left offset, and finally processes the left operand. For unary operators (NOT), it sets a fixed offset and processes the single operand. The function also detects QI_VALSTOP nodes which indicate the presence of stop words that need to be removed in a subsequent cleanup phase.

## Parameters / Member Variables
- `ptr`: Pointer to an array of QueryItem structures representing the tsquery in polish notation
- `pos`: Pointer to current position in the QueryItem array, updated as traversal progresses
- `nnodes`: Total number of nodes in the QueryItem array for bounds checking
- `needcleanup`: Pointer to boolean flag set to true if QI_VALSTOP nodes are encountered

## Dependencies
- Functions called/Symbols referenced:
  - QueryItem (tsquery node structure)
  - check_stack_depth (recursion depth protection)
  - QI_VAL, QI_VALSTOP, QI_OPR (QueryItem type constants)
  - QueryOperator (operator-specific structure)
  - OP_NOT, OP_AND, OP_OR, OP_PHRASE (operator type constants)
  - elog (error reporting function)
- Called from (representative examples):
  - findoprnd_recurse (recursive calls for processing operands)
  - findoprnd (initial entry point)

## Notes and Other Information
- Implements recursive tree traversal for polish notation tsquery structures
- Calculates left-offset fields needed for efficient query execution
- Handles different operator arities (unary NOT vs binary AND/OR/PHRASE)
- Includes stack overflow protection via check_stack_depth()
- The traversal order (right operand first, then left) is specific to polish notation processing
- Sets needcleanup flag when stop words are detected for later removal
- Essential for converting parsed tsquery into executable internal representation