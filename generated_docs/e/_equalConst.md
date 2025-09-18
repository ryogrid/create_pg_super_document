# _equalConst

## Location
src/backend/nodes/equalfuncs.c: 96 - 116

## Overview
A static comparison function that determines if two Const nodes are logically equal by comparing all their structural fields and constant values.

## Definition


## Detailed Description
The  function is part of PostgreSQL's node comparison infrastructure, specifically designed to compare two  nodes for equality. This function is automatically generated as part of the equal functions framework for nodes with the  attribute. 

The function performs a comprehensive field-by-field comparison of two  structures, including type information, storage characteristics, and the actual constant value. It handles NULL constants as a special case, treating all NULL constants of the same type as equal since  cannot operate on NULL values.

## Parameters / Member Variables
- : Pointer to the first Const node to compare
- : Pointer to the second Const node to compare

Returns:  if the nodes are equal,  otherwise

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for comparing scalar fields)
  -  (macro for comparing location fields)
  -  (function to compare datum values)
- Called from (representative examples):
  - [Node](../N/Node.md) equality framework (indirectly through function pointers)

## Notes and Other Information
- This function is marked as , meaning it's only accessible within the equalfuncs.c file
- The function treats all NULL constants of the same type as equal, which is a deliberate design choice
- Uses specialized comparison macros (, ) for consistent field comparison
- Part of the custom equality checking for nodes that have the  attribute
- The comparison includes type information (consttype, consttypmod, constcollid), storage characteristics (constlen, constbyval), and the actual value
- For non-NULL constants, delegates to  for the actual value comparison, taking into account whether the type is passed by value or reference