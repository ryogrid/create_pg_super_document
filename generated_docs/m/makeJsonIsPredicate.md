# makeJsonIsPredicate

## Location
[src/backend/nodes/makefuncs.c:958-976](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L958-L976)

## Overview
Creates a JsonIsPredicate node for representing JSON type predicate expressions that test whether a value is of a specific JSON type.

## Definition
```c
Node *makeJsonIsPredicate(Node *expr, JsonFormat *format, JsonValueType item_type, bool unique_keys, int location)
```

## Detailed Description
The `makeJsonIsPredicate` function is a constructor that creates and initializes a `JsonIsPredicate` node. This node type is used in PostgreSQL's SQL/JSON implementation to represent IS JSON predicates that test whether an expression evaluates to a value of a specific JSON type (e.g., IS JSON OBJECT, IS JSON ARRAY, etc.). The function sets up all the necessary components including the expression to test, format specifications, the expected JSON type, uniqueness constraints for keys, and location information for error reporting.

## Parameters / Member Variables
- `expr`: A Node pointer to the expression that will be tested for JSON type conformance
- `format`: A JsonFormat pointer specifying the format requirements for the JSON test
- `item_type`: A JsonValueType enum value indicating the specific JSON type to test for (e.g., object, array, scalar)
- `unique_keys`: A boolean flag indicating whether JSON object keys must be unique in the test
- `location`: An integer representing the source location in the original query for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (PostgreSQL node allocation macro)
  - [JsonIsPredicate](../J/JsonIsPredicate.md) (node type structure)
  - [JsonFormat](../J/JsonFormat.md) (format specification structure)
  - JsonValueType (enum for JSON value types)
- Called from (representative examples):
  - [transformJsonIsPredicate](../t/transformJsonIsPredicate.md) (parser/parse_expr.c:4104)

## Notes and Other Information
This function is crucial for implementing SQL/JSON standard IS JSON predicates in PostgreSQL. These predicates allow users to test whether data conforms to specific JSON structural requirements before processing. The unique_keys parameter is particularly important for JSON objects, as the SQL/JSON standard requires that object keys be unique. The location parameter enables precise error reporting when predicate evaluation fails, helping users identify problematic expressions in their queries.

## Simplified Source

```c
Node *
makeJsonIsPredicate(Node *expr, JsonFormat *format, JsonValueType item_type,
                   bool unique_keys, int location) {
    // Create and initialize JsonIsPredicate node
    JsonIsPredicate *n = makeNode(JsonIsPredicate);

    // Set all the predicate properties
    n->expr = expr;           // Expression to test
    n->format = format;       // JSON format specification
    n->item_type = item_type; // Expected JSON type (object, array, etc.)
    n->unique_keys = unique_keys; // Whether object keys must be unique
    n->location = location;   // Source location for error reporting

    return (Node *) n;
}
```