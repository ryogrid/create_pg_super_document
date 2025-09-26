# JsonArrayQueryConstructor

## Location
[src/include/nodes/parsenodes.h:1947-1955](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1947-L1955)

## Overview
JsonArrayQueryConstructor represents the untransformed representation of the JSON_ARRAY(subquery) constructor function in PostgreSQL's SQL/JSON implementation, used to build JSON arrays from subquery results.

## Definition
typedef struct JsonArrayQueryConstructor
{
    NodeTag      type;
    Node        *query;             /* subquery */
    JsonOutput  *output;            /* RETURNING clause, if specified  */
    JsonFormat  *format;            /* FORMAT clause for subquery, if specified */
    bool         absent_on_null;    /* skip NULL elements? */
    ParseLoc     location;          /* token location, or -1 if unknown */
} JsonArrayQueryConstructor;

## Detailed Description
JsonArrayQueryConstructor is a parse tree node that represents a JSON_ARRAY() constructor call with a subquery before transformation. This structure is part of PostgreSQL's SQL/JSON standard implementation, specifically handling the construction of JSON arrays from the results of a subquery. The structure includes options for handling null values, input format specifications, and optional output formatting specifications.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a JsonArrayQueryConstructor node
- `query`: Pointer to the subquery node that provides the array elements
- `output`: Optional JsonOutput structure specifying RETURNING clause details for format control
- `format`: Optional JsonFormat structure specifying FORMAT clause for interpreting subquery results
- `absent_on_null`: Boolean flag indicating whether NULL values should be omitted from the resulting JSON array
- `location`: Parse location information for error reporting and debugging (-1 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - [JsonOutput](JsonOutput.md)
  - [JsonFormat](JsonFormat.md)
  - ParseLoc
- Called from (representative examples):
  - [exprLocation](../e/exprLocation.md)
  - LIST_WALK
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)
  - [transformJsonArrayQueryConstructor](../t/transformJsonArrayQueryConstructor.md)

## Notes and Other Information
- This is part of PostgreSQL's implementation of the SQL/JSON standard
- The structure is used during the parsing phase before transformation to executable form
- Distinguished from JsonArrayConstructor by using a subquery instead of explicit value expressions
- The format field allows specification of how to interpret the subquery results (e.g., as JSON or text)
- The absent_on_null flag implements the ABSENT ON NULL behavior from the SQL/JSON standard
- The subquery can return multiple rows, each contributing an element to the resulting JSON array
- The location field aids in providing accurate error messages during query compilation