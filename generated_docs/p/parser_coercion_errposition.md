# parser_coercion_errposition

## Location
src/backend/parser/parse_coerce.c: 1314 - 1343

## Overview
Reports the source location of type coercion errors with preference for explicit coercion syntax (CAST, ::) over implicit coercion locations for better error reporting.

## Definition
```c
int parser_coercion_errposition(ParseState *pstate,
                               int coerce_location,
                               Node *input_expr)
```

## Detailed Description
This function provides intelligent error position reporting for type coercion failures by selecting the most informative source location. When an explicit coercion is requested (via CAST expression or :: syntax), it prefers to point at that coercion request location to help users identify exactly where the problematic coercion was attempted. However, for implicit coercions where no explicit coercion syntax exists, it falls back to pointing at the input expression location.

The function serves as a specialized wrapper around `parser_errposition`, designed specifically for coercion error contexts. It enhances error messages by providing more precise location information, making it easier for users to understand and fix type-related errors in their SQL queries.

## Parameters / Member Variables
- `pstate`: ParseState pointer containing parser context and error reporting information
- `coerce_location`: Source location of the explicit coercion request (-1 if no explicit coercion)
- `input_expr`: The input expression node being coerced (used as fallback location)

## Dependencies
- Functions called/Symbols referenced:
  - parser_errposition (called twice with different locations)
  - exprLocation
- Called from (representative examples):
  - coerce_record_to_complex (src/backend/parser/parse_coerce.c:1053, 1092, 1113, 1125)
  - transformTypeCast (src/backend/parser/parse_expr.c:2765)
  - coerceJsonFuncExpr (src/backend/parser/parse_expr.c:3645)

## Notes and Other Information
- Designed specifically for coercion error reporting but could potentially be generalized for other parser error contexts
- Uses a simple but effective heuristic: prefer explicit syntax locations over expression locations
- Part of PostgreSQL's comprehensive error reporting system that helps users identify exact locations of SQL syntax issues
- The comment suggests this pattern might be useful beyond coercion errors and could be generalized in the future