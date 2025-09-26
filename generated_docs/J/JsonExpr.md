# JsonExpr

## Location
src/include/nodes/primnodes.h: 1813 - 1860

## Overview
JsonExpr represents the transformed representation of JSON_VALUE(), JSON_QUERY(), and JSON_EXISTS() functions in PostgreSQL's execution tree.

## Definition


## Detailed Description
JsonExpr is a node type that represents JSON path expressions after parsing and transformation. It encapsulates all the necessary information for executing JSON_VALUE(), JSON_QUERY(), and JSON_EXISTS() operations, including the source JSON data, path specification, output format requirements, error handling behaviors, and various execution parameters.

## Parameters / Member Variables
- : Base Expr structure for expression tree integration
- : JsonExprOp specifying the type of JSON operation (VALUE, QUERY, EXISTS)
- : Name of JSON_TABLE() column, or NULL for standalone expressions
- : The jsonb-valued expression to be queried
- : JsonFormat specification for the input expression format
- : The jsonpath-valued expression containing the query pattern
- : JsonReturning specification for expected output type and format
- : List of parameter names for PASSING clause
- : List of parameter values for PASSING clause
- : JsonBehavior defining action when no results are found
- : JsonBehavior defining action when errors occur
- : Flag indicating whether I/O coercion should be used for type conversion
- : Flag indicating whether JSON-specific coercion should be used
- : JsonWrapper specification for JSON_QUERY result wrapping
- : Flag to keep or omit quotes for singleton scalars in JSON_QUERY()
- : Collation OID for string operations
- : Parse location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - JsonExprOp
  - JsonFormat
  - JsonReturning
  - JsonBehavior
  - JsonWrapper
  - ParseLoc
- Called from (representative examples):
  - ExecInitExprRec
  - ExecInitJsonExpr
  - transformJsonFuncExpr
  - transformJsonTable

## Notes and Other Information
- Central to PostgreSQL's SQL/JSON functionality implementation
- Supports all major JSON path operations defined in SQL standard
- Integrates with PostgreSQL's expression evaluation framework
- Handles complex error and empty result behaviors as specified in SQL/JSON standard
- Used both in standalone JSON expressions and as part of JSON_TABLE operations