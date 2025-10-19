# makeJsonKeyValue

## Location
[src/backend/nodes/makefuncs.c:943-957](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L943-L957)

## Overview
Creates a JsonKeyValue node for representing key-value pairs in JSON object construction within PostgreSQL's SQL/JSON implementation.

## Definition
```c
Node *makeJsonKeyValue(Node *key, Node *value)
```

## Detailed Description
The `makeJsonKeyValue` function is a constructor that creates and initializes a `JsonKeyValue` node. This node type represents a key-value pair used in JSON object construction operations. The function allocates memory for a new `JsonKeyValue` structure and sets up the key expression and value expression components. The key is cast to an `Expr` type while the value is specifically cast to a `JsonValueExpr` type, ensuring type safety in the JSON construction process.

## Parameters / Member Variables
- `key`: A Node pointer representing the expression that will produce the key for the JSON key-value pair
- `value`: A Node pointer representing the value expression, which must be castable to JsonValueExpr

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (PostgreSQL node allocation macro)
  - castNode (PostgreSQL type casting macro)
  - [JsonKeyValue](../J/JsonKeyValue.md) (node type structure)
  - [JsonValueExpr](../J/JsonValueExpr.md) (value expression node type)
- Called from (representative examples):
  - Referenced in makefuncs.h header file

## Notes and Other Information
This function is essential for constructing JSON objects in PostgreSQL's SQL/JSON implementation. The strict typing enforced by casting the value to JsonValueExpr ensures that only properly formatted JSON values can be used in key-value pairs. The function returns a generic Node pointer, which allows it to be used in various contexts where JSON key-value pairs are needed. The use of castNode indicates that runtime type checking is performed to ensure the value parameter is indeed a JsonValueExpr.

## Simplified Source

```c
Node *
makeJsonKeyValue(Node *key, Node *value)
{
    JsonKeyValue *n = makeNode(JsonKeyValue);

    // Set key and value with appropriate type casting
    n->key = (Expr *) key;
    n->value = castNode(JsonValueExpr, value);

    return (Node *) n;
}
```