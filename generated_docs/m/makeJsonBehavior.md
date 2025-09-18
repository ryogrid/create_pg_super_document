# makeJsonBehavior

## Location
src/backend/nodes/makefuncs.c: 927 - 942

## Overview
Creates a JsonBehavior node for specifying behavior handling in SQL/JSON operations, such as error handling and null handling behaviors.

## Definition
```c
JsonBehavior *makeJsonBehavior(JsonBehaviorType btype, Node *expr, int location)
```

## Detailed Description
The `makeJsonBehavior` function is a constructor that creates and initializes a `JsonBehavior` node. This node type is used in PostgreSQL's SQL/JSON implementation to specify how certain situations should be handled during JSON processing, such as what to do when errors occur or when null values are encountered. The function allocates memory for a new `JsonBehavior` structure and sets up its behavior type, associated expression, and source location information.

## Parameters / Member Variables
- `btype`: A JsonBehaviorType enum value specifying the type of behavior (e.g., error handling, null handling)
- `expr`: A Node pointer to an expression that may be used as part of the behavior specification
- `location`: An integer representing the source location in the original query for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (PostgreSQL node allocation macro)
  - JsonBehavior (node type structure)
  - JsonBehaviorType (enum for behavior types)
- Called from (representative examples):
  - transformJsonBehavior (parser/parse_expr.c:4823)

## Notes and Other Information
This function is essential for implementing SQL/JSON standard compliance in PostgreSQL, particularly for handling the ON ERROR and ON EMPTY clauses that can be specified in JSON functions. The behavior nodes created by this function define how the system should respond to various exceptional conditions during JSON processing. The location parameter is important for providing accurate error messages that point to the correct position in the original SQL query.