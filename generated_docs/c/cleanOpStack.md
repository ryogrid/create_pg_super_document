# cleanOpStack

## Location
src/backend/utils/adt/tsquery.c: 648 - 671

## Overview
A function that processes and cleans the operator stack by popping operators with higher or equal precedence and converting them to postfix notation during tsquery parsing.

## Definition


## Detailed Description
The cleanOpStack function implements operator precedence handling in tsquery parsing by processing the operator stack. It pops operators from the stack that have higher precedence than the current operator (or equal precedence for right-associative NOT operator) and converts them to postfix notation using pushOperator. This ensures proper operator precedence and associativity rules are maintained during query parsing. The function handles the special case of the NOT operator which is right-associative, unlike other operators which are left-associative.

## Parameters / Member Variables
- `state`: TSQueryParserState structure containing parser state information
- `stack`: Pointer to an array of OperatorElement structures representing the operator stack
- `lenstack`: Pointer to an integer tracking the current length/size of the stack
- `op`: The current operator (int8) being processed for precedence comparison

## Dependencies
- Functions called/Symbols referenced:
  - TSQueryParserState (parser state structure)
  - OperatorElement (stack element structure)
  - OP_PRIORITY (macro for operator precedence)
  - OP_NOT (NOT operator constant)
  - pushOperator (function to output operators in postfix form)
- Called from (representative examples):
  - makepol (multiple times during parsing)

## Notes and Other Information
- Implements proper operator precedence rules for tsquery expressions
- Handles right-associativity of NOT operator specifically
- Essential for converting infix notation to postfix/polish notation
- Works together with pushOpStack to manage the operator stack
- The function modifies the stack by popping elements and decrements lenstack accordingly