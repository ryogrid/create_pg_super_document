# transformJsonFuncExpr

## Location
src/backend/parser/parse_expr.c: 4271 - 4636

## Overview
Transforms JSON_VALUE, JSON_QUERY, JSON_EXISTS, and JSON_TABLE function expressions into JsonExpr nodes with comprehensive validation and behavior handling.

## Definition
```c
static Node *transformJsonFuncExpr(ParseState *pstate, JsonFuncExpr *func)
```

## Detailed Description
The transformJsonFuncExpr function is a comprehensive transformation handler for SQL/JSON query functions. It processes four major JSON functions: JSON_EXISTS (returns boolean), JSON_QUERY (returns JSON), JSON_VALUE (returns scalar), and JSON_TABLE (returns table). The function performs extensive validation of ON EMPTY and ON ERROR behavior clauses, ensuring they are appropriate for each specific JSON function type. It handles format specifications, quote behavior, wrapper settings, and type coercion. The function creates a JsonExpr node with all necessary configuration for runtime execution, including path specifications, passing arguments, and return type handling.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and state information
- `func`: JsonFuncExpr node representing the parsed JSON function expression (VALUE/QUERY/EXISTS/TABLE)

## Dependencies
- Functions called/Symbols referenced:
  - transformJsonValueExpr
  - transformExprRecurse
  - coerce_to_target_type
  - transformJsonPassingArgs
  - transformJsonOutput
  - transformJsonBehavior
  - makeNode
  - get_typtype
  - DomainHasConstraints
  - ereport
  - parser_errposition
  - exprLocation
  - exprType
  - format_type_be
  - Various JSON behavior constants (JSON_BEHAVIOR_ERROR, JSON_BEHAVIOR_NULL, etc.)
  - Various JSON format constants (JS_FORMAT_JSONB, JS_FORMAT_DEFAULT, etc.)
  - JSON operation constants (JSON_EXISTS_OP, JSON_QUERY_OP, JSON_VALUE_OP, JSON_TABLE_OP)
- Called from (representative examples):
  - transformExprRecurse

## Notes and Other Information
This is one of the most complex transformation functions in PostgreSQL's JSON support, handling multiple function types with different semantics. It includes extensive validation logic for behavior clauses, ensuring that each JSON function only accepts appropriate ON EMPTY/ON ERROR behaviors. The function handles type coercion strategies differently for each operation type and manages format specifications and quote behavior. Located at src/backend/parser/parse_expr.c:4271-4636.