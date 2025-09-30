# transformBooleanTest

## Location
[src/backend/parser/parse_expr.c:2528-2567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2528-L2567)

## Overview
Transforms Boolean test expressions (IS TRUE, IS FALSE, IS UNKNOWN, etc.) during parsing by applying proper type coercion to ensure the argument is boolean-compatible.

## Definition

```c
static Node *
transformBooleanTest(ParseState *pstate, BooleanTest *b)
```
## Detailed Description
The  function processes Boolean test expressions during the parsing phase. It handles six types of Boolean tests: IS TRUE, IS NOT TRUE, IS FALSE, IS NOT FALSE, IS UNKNOWN, and IS NOT UNKNOWN. The function first determines the appropriate clause name for error reporting based on the test type, then recursively transforms the argument expression and coerces it to boolean type.

The function ensures type safety by converting the argument to a boolean-compatible type using , which handles implicit conversions from various data types to boolean. If an unsupported boolean test type is encountered, it logs an error.

## Parameters / Member Variables
- : ParseState context for the current parsing operation
- : Input BooleanTest node containing the test type and argument expression

## Dependencies
- Functions called/Symbols referenced:
  - [transformExprRecurse](transformExprRecurse.md)
  - [coerce_to_boolean](../c/coerce_to_boolean.md)
  - elog
  - IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN (enum constants)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- Supports all six standard SQL Boolean test operations
- Modifies the input BooleanTest node in place rather than creating a new one
- Uses the clause name for informative error messages during type coercion
- The function ensures the argument can be evaluated as a boolean value
- Located in src/backend/parser/parse_expr.c:2528-2567

## Simplified Source

```c
static Node *
transformBooleanTest(ParseState *pstate, BooleanTest *b)
{
    const char *clausename;

    // Determine clause name for error messages
    switch (b->booltesttype) {
        case IS_TRUE:
            clausename = "IS TRUE";
            break;
        case IS_NOT_TRUE:
            clausename = "IS NOT TRUE";
            break;
        case IS_FALSE:
            clausename = "IS FALSE";
            break;
        case IS_NOT_FALSE:
            clausename = "IS NOT FALSE";
            break;
        case IS_UNKNOWN:
            clausename = "IS UNKNOWN";
            break;
        case IS_NOT_UNKNOWN:
            clausename = "IS NOT UNKNOWN";
            break;
        default:
            elog(ERROR, "unrecognized booltesttype: %d", (int) b->booltesttype);
            clausename = NULL;
    }

    // Transform and coerce argument to boolean
    b->arg = (Expr *) transformExprRecurse(pstate, (Node *) b->arg);
    b->arg = (Expr *) coerce_to_boolean(pstate, (Node *) b->arg, clausename);

    return (Node *) b;
}
```