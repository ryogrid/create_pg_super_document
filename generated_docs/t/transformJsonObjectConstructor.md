# transformJsonObjectConstructor

## Location
[src/backend/parser/parse_expr.c:3714-3750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3714-L3750)

## Overview
Transforms JSON_OBJECT() constructor expressions into JsonConstructorExpr nodes for PostgreSQL's JSON object construction functionality.

## Definition

```c
static Node *
transformJsonObjectConstructor(ParseState *pstate, JsonObjectConstructor *ctor)
```
## Detailed Description
This function is responsible for transforming JSON_OBJECT() constructor syntax into PostgreSQL's internal representation as a JsonConstructorExpr node with type JSCTOR_JSON_OBJECT. The function processes key-value pairs from the constructor expression, transforms each key and value through appropriate transformation functions, and creates the final JSON constructor expression with proper formatting and output coercion.

The transformation process involves:
1. Processing each key-value pair in the constructor's expression list
2. Transforming keys using the standard expression transformation
3. Transforming values using specialized JSON value transformation with default formatting
4. Creating arguments list with alternating key-value pairs
5. Applying output transformation and formatting rules
6. Constructing the final JsonConstructorExpr node with all necessary flags and settings

## Parameters / Member Variables
- : ParseState pointer containing current parsing context and state information
- : JsonObjectConstructor pointer containing the source JSON_OBJECT() constructor expression to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - castNode (for JsonKeyValue casting)
  - [transformExprRecurse](transformExprRecurse.md) (for key transformation)
  - [transformJsonValueExpr](transformJsonValueExpr.md) (for value transformation with JSON_OBJECT context)
  - [transformJsonConstructorOutput](transformJsonConstructorOutput.md) (for output formatting)
  - [makeJsonConstructorExpr](../m/makeJsonConstructorExpr.md) (for creating the final expression node)
  - JS_FORMAT_DEFAULT (default JSON formatting constant)
  - JSCTOR_JSON_OBJECT (JSON constructor type constant)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- The function handles both empty JSON_OBJECT() calls and those with key-value pairs
- Key-value pairs are processed in order and stored as alternating entries in the arguments list
- The function respects constructor flags like unique key constraints, null handling behavior, and absent value handling
- All transformations maintain proper location information for error reporting
- The result is automatically coerced to the target type specified in the constructor's output specification

## Simplified Source

```c
static Node *transformJsonObjectConstructor(ParseState *pstate, JsonObjectConstructor *ctor) {
    List *args = NIL;

    // Transform key-value pairs if any exist
    if (ctor->exprs) {
        ListCell *lc;
        foreach(lc, ctor->exprs) {
            JsonKeyValue *kv = castNode(JsonKeyValue, lfirst(lc));

            // Transform key and value expressions
            Node *key = transformExprRecurse(pstate, (Node *) kv->key);
            Node *val = transformJsonValueExpr(pstate, "JSON_OBJECT()",
                                              kv->value, JS_FORMAT_DEFAULT,
                                              InvalidOid, false);

            // Add key-value pair to arguments list
            args = lappend(args, key);
            args = lappend(args, val);
        }
    }

    // Apply output transformation and formatting
    JsonReturning *returning = transformJsonConstructorOutput(pstate, ctor->output, args);

    // Create the JSON constructor expression
    return makeJsonConstructorExpr(pstate, JSCTOR_JSON_OBJECT, args, NULL,
                                  returning, ctor->unique,
                                  ctor->absent_on_null, ctor->location);
}
```