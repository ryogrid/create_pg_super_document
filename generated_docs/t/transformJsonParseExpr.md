# transformJsonParseExpr

## Location
[src/backend/parser/parse_expr.c:4153-4201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L4153-L4201)

## Overview
Transforms a JSON() expression into a JsonConstructorExpr node that validates and parses JSON input with optional unique key checking.

## Definition
```c
static Node *transformJsonParseExpr(ParseState *pstate, JsonParseExpr *jsexpr)
```

## Detailed Description
This function processes SQL JSON() expressions during parsing, creating JsonConstructorExpr nodes with type JSCTOR_JSON_PARSE. The function handles two distinct execution paths: one for expressions with UNIQUE KEYS clause requiring text input and key uniqueness validation, and another for standard JSON parsing with type coercion. When unique_keys is enabled, it validates that the input is text type and performs key uniqueness checking during execution. Otherwise, it uses standard JSON value transformation for compatibility with PostgreSQL cast operations.

## Parameters / Member Variables
- `pstate`: ParseState pointer containing parsing context and state information
- `jsexpr`: JsonParseExpr pointer containing the JSON expression specification including output, expression, and unique keys option

## Dependencies
- Functions called/Symbols referenced:
  - [transformJsonReturning](transformJsonReturning.md)
  - [transformJsonParseArg](transformJsonParseArg.md)  
  - [transformJsonValueExpr](transformJsonValueExpr.md)
  - [makeJsonConstructorExpr](../m/makeJsonConstructorExpr.md)
  - list_make1
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md) (src/backend/parser/parse_expr.c:360)

## Notes and Other Information
- Creates JSCTOR_JSON_PARSE type JsonConstructorExpr nodes for JSON validation
- WITH UNIQUE KEYS clause requires TEXT input type and enables key uniqueness checking
- Without unique keys, uses standard JSON value transformation for PostgreSQL cast compatibility
- Uses JS_FORMAT_JSON as the default format for non-unique key processing
- Error reporting includes specific position information for debugging
- Part of SQL/JSON standard implementation for JSON() constructor expressions
- The unique_keys parameter controls execution-time behavior for key validation

## Simplified Source

```c
static Node *transformJsonParseExpr(ParseState *pstate, JsonParseExpr *jsexpr) {
    // Transform output returning clause
    JsonReturning *returning = transformJsonReturning(pstate, jsexpr->output, "JSON()");

    Node *arg;
    if (jsexpr->unique_keys) {
        // For unique keys: require TEXT input and use special parsing
        JsonValueExpr *jve = jsexpr->expr;
        Oid arg_type;

        arg = transformJsonParseArg(pstate, (Node *) jve->raw_expr,
                                   jve->format, &arg_type);

        // Ensure input is text type for unique key checking
        if (arg_type != TEXTOID) {
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("cannot use non-string types with WITH UNIQUE KEYS clause"),
                           parser_errposition(pstate, jsexpr->location)));
        }
    } else {
        // Standard JSON parsing with type coercion
        arg = transformJsonValueExpr(pstate, "JSON()", jsexpr->expr,
                                    JS_FORMAT_JSON, returning->typid, false);
    }

    // Create the JSON parse constructor expression
    return makeJsonConstructorExpr(pstate, JSCTOR_JSON_PARSE, list_make1(arg), NULL,
                                  returning, jsexpr->unique_keys, false,
                                  jsexpr->location);
}
```