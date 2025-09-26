# makeJsonConstructorExpr

## Location
[src/backend/parser/parse_expr.c:3654-3713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3654-L3713)

## Overview
Creates a JsonConstructorExpr node for representing JSON constructor functions in PostgreSQL's expression tree, including automatic coercion setup for return type conversion.

## Definition
```c
static Node *makeJsonConstructorExpr(ParseState *pstate, JsonConstructorType type,
                                     List *args, Expr *fexpr, JsonReturning *returning,
                                     bool unique, bool absent_on_null, int location)
```

## Detailed Description
This function creates a JsonConstructorExpr node, which represents JSON constructor functions (JSON_OBJECT, JSON_ARRAY, etc.) in PostgreSQL's internal expression representation. Key functionality includes:

1. **Node Creation**: Constructs and populates a JsonConstructorExpr with all necessary metadata
2. **Placeholder Management**: Creates CaseTestExpr nodes as placeholders for the JSON result that will be coerced to the target type
3. **Type Inference**: When no function expression is provided, infers the JSON type (JSON vs JSONB) based on the returning format
4. **Coercion Setup**: Automatically sets up type coercion from the JSON result to the specified return type using coerceJsonFuncExpr

The function uses CaseTestExpr as a creative placeholder mechanism to represent the intermediate JSON value that will be produced at runtime.

## Parameters / Member Variables
- `pstate`: ParseState pointer for parser context and error reporting
- `type`: JsonConstructorType indicating the specific constructor (OBJECT, ARRAY, etc.)
- `args`: List of argument expressions for the constructor
- `fexpr`: Optional function expression; if NULL, type is inferred from returning format
- `returning`: JsonReturning structure specifying output type and format requirements
- `unique`: Boolean indicating whether duplicate keys should cause errors (for objects)  
- `absent_on_null`: Boolean controlling NULL value handling behavior
- `location`: Parse location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [exprCollation](../e/exprCollation.md)  
  - [coerceJsonFuncExpr](../c/coerceJsonFuncExpr.md)
- Called from (representative examples):
  - [transformJsonObjectConstructor](../t/transformJsonObjectConstructor.md)
  - [transformJsonAggConstructor](../t/transformJsonAggConstructor.md)
  - [transformJsonArrayConstructor](../t/transformJsonArrayConstructor.md)
  - [transformJsonParseExpr](../t/transformJsonParseExpr.md)
  - [transformJsonScalarExpr](../t/transformJsonScalarExpr.md)
  - [transformJsonSerializeExpr](../t/transformJsonSerializeExpr.md)

## Notes and Other Information
- This is a static function used internally within PostgreSQL's JSON expression parsing
- Uses CaseTestExpr as a clever placeholder mechanism to represent runtime JSON values for coercion
- Automatically determines whether the result should be JSON or JSONB based on the returning format specification
- The coercion field is only set when actual type coercion is needed (coercion != placeholder)
- Part of PostgreSQL's comprehensive SQL/JSON standard implementation
- Handles both cases where a specific function expression is provided and where type inference is needed