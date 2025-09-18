# transformJsonSerializeExpr

## Location
src/backend/parser/parse_expr.c: 4225 - 4270

## Overview
Transforms a JSON_SERIALIZE() expression into a JsonConstructorExpr node that converts JSON values into character or bytea strings.

## Definition
```c
static Node *transformJsonSerializeExpr(ParseState *pstate, JsonSerializeExpr *expr)
```

## Detailed Description
The transformJsonSerializeExpr function handles the transformation of JSON_SERIALIZE() SQL expressions during parsing. JSON_SERIALIZE() is a SQL/JSON function that converts JSON values into string representations (text or bytea format). The function creates a JsonConstructorExpr node of type JSCTOR_JSON_SERIALIZE to perform the serialization at execution time. It validates that the RETURNING clause specifies either a string type or bytea, rejecting other data types. If no RETURNING clause is specified, it defaults to TEXT FORMAT JSON. The input JSON expression is processed through transformJsonValueExpr to ensure proper JSON formatting.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and state information
- `expr`: JsonSerializeExpr node representing the parsed JSON_SERIALIZE() expression from the SQL query

## Dependencies
- Functions called/Symbols referenced:
  - transformJsonValueExpr
  - transformJsonOutput
  - get_type_category_preferred
  - makeJsonFormat
  - makeJsonConstructorExpr
  - format_type_be
  - ereport
  - makeNode
  - JS_FORMAT_JSON
  - JS_ENC_DEFAULT
  - JSCTOR_JSON_SERIALIZE
  - TYPCATEGORY_STRING
  - JsonReturning
- Called from (representative examples):
  - transformExprRecurse

## Notes and Other Information
This function is part of PostgreSQL's SQL/JSON support implementation. It enforces strict type checking for the RETURNING clause, ensuring only string types or bytea are allowed. The function provides detailed error messages when invalid types are specified in the RETURNING clause, helping users understand the correct usage. Located at src/backend/parser/parse_expr.c:4225-4270.