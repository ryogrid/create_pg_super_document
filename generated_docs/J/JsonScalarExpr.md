# JsonScalarExpr

## Location
src/include/nodes/parsenodes.h: 1896 - 1902

## Overview
JsonScalarExpr represents the untransformed representation of the JSON_SCALAR() function call in PostgreSQL's SQL/JSON implementation, used to convert scalar values to JSON format.

## Definition


## Detailed Description
JsonScalarExpr is a parse tree node that represents a JSON_SCALAR() function call before transformation. This structure is part of PostgreSQL's SQL/JSON standard implementation, specifically handling the conversion of scalar values to JSON format. The structure maintains the original scalar expression along with optional output formatting specifications and location information for error reporting.

## Parameters / Member Variables
- : NodeTag identifying this as a JsonScalarExpr node
- : Pointer to the scalar expression that will be converted to JSON
- : Optional JsonOutput structure specifying RETURNING clause details for format control
- : Parse location information for error reporting and debugging (-1 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - JsonOutput
  - ParseLoc
- Called from (representative examples):
  - raw_expression_tree_walker_impl
  - transformExprRecurse
  - transformJsonScalarExpr

## Notes and Other Information
- This is part of PostgreSQL's implementation of the SQL/JSON standard
- The structure is used during the parsing phase before transformation to executable form
- The location field aids in providing accurate error messages during query compilation
- The output field allows for flexible JSON formatting options through the RETURNING clause