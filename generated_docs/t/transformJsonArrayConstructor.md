# transformJsonArrayConstructor

## Location
src/backend/parser/parse_expr.c: 4010 - 4039

## Overview
Transforms a JSON_ARRAY() constructor into a JsonConstructorExpr node for array creation during SQL parsing.

## Definition
```c
static Node *transformJsonArrayConstructor(ParseState *pstate, JsonArrayConstructor *ctor)
```

## Detailed Description
This function processes SQL JSON_ARRAY() constructor expressions during the parsing phase. It transforms the constructor into a JsonConstructorExpr node with type JSCTOR_JSON_ARRAY, handling element expressions and output formatting. The function iterates through any provided element expressions, transforms them using transformJsonValueExpr, and constructs the final expression node that will be used during execution.

## Parameters / Member Variables
- `pstate`: ParseState pointer containing parsing context and state information
- `ctor`: JsonArrayConstructor pointer containing the array constructor specification with expressions and options

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - transformJsonValueExpr
  - transformJsonConstructorOutput
  - makeJsonConstructorExpr
  - lappend
  - lfirst
- Called from (representative examples):
  - transformExprRecurse (src/backend/parser/parse_expr.c:340)

## Notes and Other Information
- The function handles empty arrays (when ctor->exprs is NULL)
- Uses JS_FORMAT_DEFAULT for element formatting
- Element expressions are wrapped in JsonValueExpr nodes
- The resulting expression includes null handling behavior controlled by ctor->absent_on_null
- Part of PostgreSQL's JSON constructor infrastructure introduced for SQL/JSON standard compliance