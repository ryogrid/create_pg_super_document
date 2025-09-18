# ParseExprKindName

## Location
[src/backend/parser/parse_expr.c:3121-3226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3121-L3226)

## Overview
Returns a human-readable string description for a ParseExprKind enumeration value, primarily used for error reporting and debugging.

## Definition
```c
const char *ParseExprKindName(ParseExprKind exprKind)
```

## Detailed Description
This function maps ParseExprKind enumeration values to descriptive string names that identify the context where an expression is being used. It serves as a central translation facility for converting internal expression context codes into user-friendly error messages and debugging output. The function covers all possible expression contexts in PostgreSQL's SQL parser, from basic clauses like WHERE and SELECT to advanced features like window functions, partitioning, and generated columns.

The function is designed to return simple SQL keywords when practical, making error messages more recognizable to users. When no exact SQL keyword matches, it provides descriptive phrases like "index expression" or "partition bound". The function intentionally lacks a default case to ensure compiler warnings when new ParseExprKind values are added without corresponding string mappings.

## Parameters / Member Variables
- `exprKind`: The ParseExprKind enumeration value to convert to a string description

## Dependencies
- Functions called/Symbols referenced:
  - [ParseExprKind](ParseExprKind.md) (enumeration type)
  - Multiple EXPR_KIND_* constants (enumeration values)
- Called from (representative examples):
  - [check_agglevels_and_constraints](../c/check_agglevels_and_constraints.md)
  - [transformWindowFuncCall](../t/transformWindowFuncCall.md)
  - [checkTargetlistEntrySQL92](../c/checkTargetlistEntrySQL92.md)
  - [findTargetlistEntrySQL92](../f/findTargetlistEntrySQL92.md)
  - [check_srf_call_placement](../c/check_srf_call_placement.md)

## Notes and Other Information
- Used extensively for error reporting throughout the parser
- The function covers all expression contexts in PostgreSQL SQL parsing
- Intentionally lacks a default case to catch missing enum mappings at compile time
- Returns "unrecognized expression kind" for unknown values at runtime
- Many enum values share the same string representation (e.g., both EXPR_KIND_UPDATE_SOURCE and EXPR_KIND_UPDATE_TARGET return "UPDATE")
- Prioritizes SQL keyword clarity over internal implementation details in naming