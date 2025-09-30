# transformJsonIsPredicate

## Location
[src/backend/parser/parse_expr.c:4090-4112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L4090-L4112)

## Overview
Transforms an IS JSON predicate expression into a JsonIsPredicate node for validating JSON document structure.

## Definition
```c
static Node *transformJsonIsPredicate(ParseState *pstate, JsonIsPredicate *pred)
```

## Detailed Description
This function processes SQL IS JSON predicate expressions during parsing. It validates that the input expression can be used with JSON predicates by calling transformJsonParseArg for type coercion and validation. The function ensures only compatible types (TEXT, JSON, JSONB) are used in IS JSON predicates and creates the final JsonIsPredicate node. Note that the format clause is intentionally dropped in the final predicate construction.

## Parameters / Member Variables
- `pstate`: ParseState pointer containing parsing context and state information  
- `pred`: JsonIsPredicate pointer containing the predicate specification including expression, format, item type, and options

## Dependencies
- Functions called/Symbols referenced:
  - [transformJsonParseArg](transformJsonParseArg.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [format_type_be](../f/format_type_be.md)
  - [makeJsonIsPredicate](../m/makeJsonIsPredicate.md)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md) (src/backend/parser/parse_expr.c:356)

## Notes and Other Information
- Only accepts TEXT, JSON, and JSONB data types for the predicate expression
- Provides detailed error messages for incompatible data types using format_type_be
- The format clause from the original predicate is intentionally not passed to the final node
- Supports item_type and unique_keys options for specific JSON validation requirements  
- Part of SQL/JSON standard compliance for IS JSON predicates

## Simplified Source

```c
static Node *transformJsonIsPredicate(ParseState *pstate, JsonIsPredicate *pred) {
    // Parse and validate the JSON expression argument
    Oid exprtype;
    Node *expr = transformJsonParseArg(pstate, pred->expr, pred->format, &exprtype);

    // Only allow TEXT, JSON, and JSONB types
    if (exprtype != TEXTOID && exprtype != JSONOID && exprtype != JSONBOID) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                       errmsg("cannot use type %s in IS JSON predicate",
                              format_type_be(exprtype))));
    }

    // Create the final JSON predicate node (format clause is intentionally dropped)
    return makeJsonIsPredicate(expr, NULL, pred->item_type,
                              pred->unique_keys, pred->location);
}
```