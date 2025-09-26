# transformBooleanTest

## Location
src/backend/parser/parse_expr.c: 2528 - 2567

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
  - transformExprRecurse
  - coerce_to_boolean
  - elog
  - IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN (enum constants)
- Called from (representative examples):
  - transformExprRecurse

## Notes and Other Information
- Supports all six standard SQL Boolean test operations
- Modifies the input BooleanTest node in place rather than creating a new one
- Uses the clause name for informative error messages during type coercion
- The function ensures the argument can be evaluated as a boolean value
- Located in src/backend/parser/parse_expr.c:2528-2567