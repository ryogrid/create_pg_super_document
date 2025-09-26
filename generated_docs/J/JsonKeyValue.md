# JsonKeyValue

## Location
src/include/nodes/parsenodes.h: 1872 - 1877

## Overview
JsonKeyValue represents the untransformed parse tree representation of a key-value pair used in JSON object construction functions like JSON_OBJECT() and JSON_OBJECTAGG().

## Definition
```c
typedef struct JsonKeyValue
{
    NodeTag         type;
    Expr           *key;        /* key expression */
    JsonValueExpr  *value;      /* JSON value expression */
} JsonKeyValue;
```

## Detailed Description
JsonKeyValue is a simple parse node structure that encapsulates a key-value pair for JSON object construction operations. It holds both the key expression (which will be evaluated to determine the JSON object property name) and the corresponding value expression (which provides the JSON data for that property). This structure is used as a building block in JSON_OBJECT() constructor functions and JSON_OBJECTAGG() aggregate functions to represent individual properties in the resulting JSON object.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a JsonKeyValue node
- `key`: Expr representing the expression that evaluates to the JSON object property name
- `value`: JsonValueExpr representing the JSON value to be associated with the key

## Dependencies
- Functions called/Symbols referenced:
  - Expr (key expression evaluation)
  - JsonValueExpr (JSON value expression)
  - NodeTag (inherited node type system)
- Called from (representative examples):
  - makeJsonKeyValue (constructor function)
  - transformJsonObjectConstructor (JSON object construction)
  - JsonObjectAgg (aggregate function processing)
  - exprLocation (expression location tracking)

## Notes and Other Information
- Used as fundamental building blocks for JSON object construction in PostgreSQL
- The key expression typically evaluates to a string that becomes the JSON property name
- The value can be any valid JSON value expression including scalars, arrays, or nested objects
- Part of the SQL/JSON standard implementation for object construction functions
- Simple structure but essential for representing structured JSON data creation operations
- Both key and value expressions are evaluated at runtime to produce the final JSON object