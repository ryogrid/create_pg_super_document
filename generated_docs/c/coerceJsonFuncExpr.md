# coerceJsonFuncExpr

## Location
src/backend/parser/parse_expr.c: 3590 - 3653

## Overview  
Coerces JSON/JSONB function expressions to the specified output type, handling special cases like BYTEA encoding and general type conversions with appropriate error reporting.

## Definition
```c
static Node *coerceJsonFuncExpr(ParseState *pstate, Node *expr,
                               const JsonReturning *returning, bool report_error)
```

## Detailed Description
This function handles type coercion for JSON function expressions, ensuring that the result matches the specified return type. It implements several coercion strategies:

1. **No-op Cases**: Returns the expression unchanged if no return type is specified or if the expression type already matches the target type
2. **BYTEA Special Case**: For "RETURNING bytea FORMAT json", converts JSON text to BYTEA using pg_convert_to() with appropriate encoding
3. **General Coercion**: Uses assignment-level type coercion for other type conversions, particularly allowing JSON/JSONB values to be converted to string types

The function respects PostgreSQL's type coercion hierarchy and provides detailed error messages when coercion fails.

## Parameters / Member Variables
- `pstate`: ParseState pointer for parser context and error reporting
- `expr`: The expression node to be coerced (typically a JSON function result)
- `returning`: JsonReturning structure specifying the target type and format requirements
- `report_error`: Boolean controlling whether to report errors or return NULL on coercion failure

## Dependencies
- Functions called/Symbols referenced:
  - exprType
  - [exprLocation](../e/exprLocation.md)  
  - OidIsValid
  - [coerce_to_specific_type](coerce_to_specific_type.md)
  - [getJsonEncodingConst](../g/getJsonEncodingConst.md)
  - makeFuncExpr
  - list_make2
  - [coerce_to_target_type](coerce_to_target_type.md)
  - ereport (for error reporting)
  - [format_type_be](../f/format_type_be.md)
  - [parser_coercion_errposition](../p/parser_coercion_errposition.md)
- Called from (representative examples):
  - [makeJsonConstructorExpr](../m/makeJsonConstructorExpr.md)

## Notes and Other Information
- This is a static function used internally within PostgreSQL's JSON expression processing
- Handles the special case of encoding JSON text to BYTEA using pg_convert_to() function
- Uses COERCION_ASSIGNMENT level for type coercion, which enables implicit coercion with proper typmod handling
- Location tracking is carefully managed to provide accurate error positioning to users
- The function can operate in both error-reporting and silent modes based on the report_error parameter
- Primarily enables JSON/JSONB to string type conversions as part of SQL/JSON standard compliance