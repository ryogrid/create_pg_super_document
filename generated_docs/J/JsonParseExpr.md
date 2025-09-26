# JsonParseExpr

## Location
[src/include/nodes/parsenodes.h:1883-1890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1883-L1890)

## Overview
JsonParseExpr represents the untransformed parse tree representation of a JSON() function call, which parses a string as JSON and validates its structure with optional output formatting and key uniqueness constraints.

## Definition
```c
typedef struct JsonParseExpr
{
    NodeTag         type;
    JsonValueExpr  *expr;           /* string expression */
    JsonOutput     *output;         /* RETURNING clause, if specified */
    bool            unique_keys;    /* WITH UNIQUE KEYS? */
    ParseLoc        location;       /* token location, or -1 if unknown */
} JsonParseExpr;
```

## Detailed Description
JsonParseExpr represents the JSON() function which takes a string input and parses it as JSON data, validating that it contains well-formed JSON. The function can optionally enforce unique key constraints within JSON objects and supports a RETURNING clause to specify the output format. This structure is part of the SQL/JSON standard implementation and provides a way to validate and optionally reformat JSON strings while ensuring they conform to JSON syntax rules.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a JsonParseExpr node
- `expr`: JsonValueExpr representing the input string expression to be parsed as JSON
- `output`: JsonOutput structure specifying the RETURNING clause format, if specified
- `unique_keys`: Boolean flag indicating whether WITH UNIQUE KEYS constraint is applied
- `location`: ParseLoc for tracking the position in the source query

## Dependencies
- Functions called/Symbols referenced:
  - JsonValueExpr (input string expression)
  - JsonOutput (output formatting specification)
  - ParseLoc (location tracking)
  - NodeTag (inherited node type system)
- Called from (representative examples):
  - transformExprRecurse (expression transformation)
  - transformJsonParseExpr (specific JSON parse transformation)
  - raw_expression_tree_walker_impl (parse tree walking)

## Notes and Other Information
- Implements the SQL/JSON JSON() function for parsing and validating JSON strings
- The unique_keys flag enforces that JSON objects cannot contain duplicate property names
- RETURNING clause allows specification of output format and data type conversion
- Provides JSON validation at query time, ensuring input strings are well-formed JSON
- Part of PostgreSQL's comprehensive SQL/JSON standard compliance
- Can be used both for validation and format conversion of JSON data
- Errors during JSON parsing can be handled through the broader JSON error handling framework