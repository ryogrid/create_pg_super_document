# JsonObjectConstructor

## Location
[src/include/nodes/parsenodes.h:1920-1928](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1920-L1928)

## Overview
JsonObjectConstructor represents the untransformed representation of the JSON_OBJECT() constructor function in PostgreSQL's SQL/JSON implementation, used to build JSON objects from key-value pairs.

## Definition
typedef struct JsonObjectConstructor
{
    NodeTag      type;
    List        *exprs;             /* list of JsonKeyValue pairs */
    JsonOutput  *output;            /* RETURNING clause, if specified  */
    bool         absent_on_null;    /* skip NULL values? */
    bool         unique;            /* check key uniqueness? */
    ParseLoc     location;          /* token location, or -1 if unknown */
} JsonObjectConstructor;

## Detailed Description
JsonObjectConstructor is a parse tree node that represents a JSON_OBJECT() constructor call before transformation. This structure is part of PostgreSQL's SQL/JSON standard implementation, specifically handling the construction of JSON objects from a series of key-value pairs. The structure includes options for handling null values and enforcing key uniqueness, along with optional output formatting specifications.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a JsonObjectConstructor node
- `exprs`: List of JsonKeyValue pairs that define the object's content
- `output`: Optional JsonOutput structure specifying RETURNING clause details for format control
- `absent_on_null`: Boolean flag indicating whether NULL values should be omitted from the resulting JSON object
- `unique`: Boolean flag indicating whether key uniqueness should be enforced during construction
- `location`: Parse location information for error reporting and debugging (-1 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - [JsonOutput](JsonOutput.md)
  - ParseLoc
- Called from (representative examples):
  - [exprLocation](../e/exprLocation.md)
  - LIST_WALK
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)
  - [transformJsonObjectConstructor](../t/transformJsonObjectConstructor.md)

## Notes and Other Information
- This is part of PostgreSQL's implementation of the SQL/JSON standard
- The structure is used during the parsing phase before transformation to executable form
- The absent_on_null flag implements the ABSENT ON NULL behavior from the SQL/JSON standard
- The unique flag implements key uniqueness checking as specified in SQL/JSON
- The exprs list contains JsonKeyValue pairs that define the object structure
- The location field aids in providing accurate error messages during query compilation