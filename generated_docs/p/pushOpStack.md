# pushOpStack

## Location
src/backend/utils/adt/tsquery.c: 636 - 647

## Overview
A utility function that pushes an operator element onto an operator stack used during tsquery parsing in PostgreSQL's text search functionality.

## Definition


## Detailed Description
The pushOpStack function is a simple stack management utility used specifically in tsquery parsing operations. It adds a new operator element to the top of an operator stack, checking for overflow conditions. The function handles the placement of both the operator type and its associated distance value, incrementing the stack length after the push operation. This function is part of the tsquery parsing infrastructure that converts text search queries into internal representation.

## Parameters / Member Variables
- : Pointer to an array of OperatorElement structures representing the operator stack
- : Pointer to an integer tracking the current length/size of the stack
- : The operator type (int8) to be pushed onto the stack
- : The distance value (int16) associated with the operator, used for phrase queries

## Dependencies
- Functions called/Symbols referenced:
  - OperatorElement (structure type)
  - STACKDEPTH (constant for maximum stack size)
  - elog (error logging function)
- Called from (representative examples):
  - makepol

## Notes and Other Information
- Includes overflow protection by checking against STACKDEPTH constant
- Raises an internal error if stack overflow occurs
- The function is static and only used within the tsquery.c module
- Essential for maintaining operator precedence during query parsing
- Works in conjunction with cleanOpStack for complete stack management