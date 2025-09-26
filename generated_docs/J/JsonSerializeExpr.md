# JsonSerializeExpr

## Location
src/include/nodes/parsenodes.h: 1908 - 1914

## Overview
JsonSerializeExpr represents the untransformed representation of the JSON_SERIALIZE() function call in PostgreSQL's SQL/JSON implementation, used to serialize JSON values to text format.

## Definition
typedef struct JsonSerializeExpr
{
    NodeTag         type;
    JsonValueExpr  *expr;           /* json value expression */
    JsonOutput     *output;         /* RETURNING clause, if specified  */
    ParseLoc        location;       /* token location, or -1 if unknown */
} JsonSerializeExpr;

## Detailed Description
JsonSerializeExpr is a parse tree node that represents a JSON_SERIALIZE() function call before transformation. This structure is part of PostgreSQL's SQL/JSON standard implementation, specifically handling the serialization of JSON values to textual representation. The structure maintains the JSON value expression along with optional output formatting specifications and location information for error reporting.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a JsonSerializeExpr node
- `expr`: Pointer to the JsonValueExpr that will be serialized to text format
- `output`: Optional JsonOutput structure specifying RETURNING clause details for format control
- `location`: Parse location information for error reporting and debugging (-1 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - JsonValueExpr
  - JsonOutput
  - ParseLoc
- Called from (representative examples):
  - raw_expression_tree_walker_impl
  - transformExprRecurse
  - transformJsonSerializeExpr

## Notes and Other Information
- This is part of PostgreSQL's implementation of the SQL/JSON standard
- The structure is used during the parsing phase before transformation to executable form
- JSON_SERIALIZE() converts JSON values to their textual representation
- The location field aids in providing accurate error messages during query compilation
- The output field allows for flexible text formatting options through the RETURNING clause