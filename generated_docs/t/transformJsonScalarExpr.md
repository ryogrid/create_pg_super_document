# transformJsonScalarExpr

## Location
[src/backend/parser/parse_expr.c:4202-4224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L4202-L4224)

## Overview
Transforms a JSON_SCALAR() expression into a JsonConstructorExpr node that converts SQL scalar values into json[b] values.

## Definition

```c
structorExpr(pstate, JSCTOR_JSON_SCALAR, list_make1(arg), NULL,
								   returning, false, false, jsexpr->location);
```
## Detailed Description
The transformJsonScalarExpr function is responsible for transforming JSON_SCALAR() SQL expressions during the parsing phase. JSON_SCALAR() is a SQL/JSON constructor function that converts a regular SQL scalar value into a JSON representation. The function creates a JsonConstructorExpr node of type JSCTOR_JSON_SCALAR to handle the conversion at execution time. If the input expression has an unknown type, it is coerced to TEXT type before processing. The function also handles JSON output formatting and returning clauses through the transformJsonReturning helper.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and state information
- `jsexpr`: JsonScalarExpr node representing the parsed JSON_SCALAR() expression from the SQL query

## Dependencies
- Functions called/Symbols referenced:
  - [transformExprRecurse](transformExprRecurse.md)
  - [transformJsonReturning](transformJsonReturning.md)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md)
  - [makeJsonConstructorExpr](../m/makeJsonConstructorExpr.md)
  - [exprType](../e/exprType.md)
  - [JsonOutput](../J/JsonOutput.md)
  - [JsonReturning](../J/JsonReturning.md)
  - JSCTOR_JSON_SCALAR
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
This function is part of PostgreSQL's SQL/JSON support implementation. It handles the transformation of JSON_SCALAR() expressions which are used to convert SQL scalar values to JSON format. The function ensures proper type coercion for unknown types and integrates with the broader JSON expression transformation framework. Located at src/backend/parser/parse_expr.c:4202-4224.