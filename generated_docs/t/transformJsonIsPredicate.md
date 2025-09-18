# transformJsonIsPredicate

## Location
src/backend/parser/parse_expr.c: 4090 - 4112

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
  - transformJsonParseArg
  - ereport
  - errcode
  - errmsg
  - format_type_be
  - makeJsonIsPredicate
- Called from (representative examples):
  - transformExprRecurse (src/backend/parser/parse_expr.c:356)

## Notes and Other Information
- Only accepts TEXT, JSON, and JSONB data types for the predicate expression
- Provides detailed error messages for incompatible data types using format_type_be
- The format clause from the original predicate is intentionally not passed to the final node
- Supports item_type and unique_keys options for specific JSON validation requirements  
- Part of SQL/JSON standard compliance for IS JSON predicates