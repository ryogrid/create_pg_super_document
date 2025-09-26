# JsonArrayConstructor

## Location
src/include/nodes/parsenodes.h: 1934 - 1941

## Overview
JsonArrayConstructor represents the untransformed representation of the JSON_ARRAY() constructor function in PostgreSQL's SQL/JSON implementation, used to build JSON arrays from a list of elements.

## Definition
typedef struct JsonArrayConstructor
{
    NodeTag      type;
    List        *exprs;             /* list of JsonValueExpr elements */
    JsonOutput  *output;            /* RETURNING clause, if specified  */
    bool         absent_on_null;    /* skip NULL elements? */
    ParseLoc     location;          /* token location, or -1 if unknown */
} JsonArrayConstructor;

## Detailed Description
JsonArrayConstructor is a parse tree node that represents a JSON_ARRAY() constructor call before transformation. This structure is part of PostgreSQL's SQL/JSON standard implementation, specifically handling the construction of JSON arrays from a series of value expressions. The structure includes an option for handling null values and optional output formatting specifications.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a JsonArrayConstructor node
- `exprs`: List of JsonValueExpr elements that define the array's content
- `output`: Optional JsonOutput structure specifying RETURNING clause details for format control
- `absent_on_null`: Boolean flag indicating whether NULL values should be omitted from the resulting JSON array
- `location`: Parse location information for error reporting and debugging (-1 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - JsonOutput
  - ParseLoc
- Called from (representative examples):
  - exprLocation
  - LIST_WALK
  - raw_expression_tree_walker_impl
  - transformExprRecurse
  - transformJsonArrayConstructor

## Notes and Other Information
- This is part of PostgreSQL's implementation of the SQL/JSON standard
- The structure is used during the parsing phase before transformation to executable form
- The absent_on_null flag implements the ABSENT ON NULL behavior from the SQL/JSON standard
- The exprs list contains JsonValueExpr elements that define the array content
- Unlike JsonObjectConstructor, this structure doesn't need a uniqueness check since arrays allow duplicate values
- The location field aids in providing accurate error messages during query compilation