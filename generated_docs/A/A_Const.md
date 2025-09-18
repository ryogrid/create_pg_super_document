# A_Const

## Location
src/include/nodes/parsenodes.h: 357 - 365

## Overview
A_Const represents a constant value in PostgreSQL's parse tree, used to store literal values encountered during SQL parsing such as numbers, strings, booleans, and NULL constants.

## Definition


## Detailed Description
A_Const is a fundamental parse tree node that encapsulates constant values found in SQL statements. It serves as a container for various types of literal values including integers, floats, strings, booleans, and NULL values. The structure is designed to preserve both the value and its location in the original SQL text for error reporting and debugging purposes. The node includes custom attributes for copying, equality checking, and query jumbling operations.

## Parameters / Member Variables
- : NodeTag identifying this as an A_Const node
- : Union containing the actual constant value (ValUnion type)
- : Boolean flag indicating whether this represents a SQL NULL constant
- : ParseLoc storing the token's position in the source SQL, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - ValUnion
  - ParseLoc
- Called from (representative examples):
  - _copyA_Const
  - _equalA_Const
  - makeStringConst
  - exprLocation
  - transformExprRecurse
  - make_const

## Notes and Other Information
- A_Const nodes are created during parsing and are typically transformed into Const nodes during the analysis phase
- The pg_node_attr annotation indicates special handling for copy, equality, read/write, and query jumbling operations
- Location information is crucial for providing accurate error messages when parsing fails
- The ValUnion allows storage of different data types in a memory-efficient manner