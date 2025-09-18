# transformJsonValueExpr

## Location
src/backend/parser/parse_expr.c: 3288 - 3449

## Overview
Transforms a JSON value expression with format handling and type coercion, returning either the coerced raw expression or a JsonValueExpr with formatted expression.

## Definition
```c
static Node *transformJsonValueExpr(ParseState *pstate, const char *constructName,
                                   JsonValueExpr *ve, JsonFormatType default_format,
                                   Oid targettype, bool isarg)
```

## Detailed Description
This complex function handles the transformation of JSON value expressions, applying appropriate formatting and type coercion based on the specified JSON format and target type. It implements sophisticated logic to determine when to format expressions, when to allow direct passthrough of certain types, and how to handle encoding specifications.

The function performs several key operations:
1. Transforms the raw expression and handles UNKNOWN type coercion to TEXT
2. Validates encoding specifications (only allowed for bytea input)
3. Implements special handling for PASSING arguments, allowing direct passthrough of types supported by GetJsonPathVar()/JsonItemFromDatum()
4. Applies appropriate formatting based on format specifications and target types
5. Performs type coercion using either direct casting or to_json()/to_jsonb() functions
6. Returns either the transformed raw expression or a JsonValueExpr with both raw and formatted expressions

The function includes extensive error checking for invalid combinations of formats, encodings, and input types.

## Parameters / Member Variables
- `pstate`: ParseState context for the current parsing operation
- `constructName`: Name of the JSON construct being processed (for error messages)
- `ve`: JsonValueExpr containing the raw expression and format specifications
- `default_format`: Default JSON format type to use when none is specified
- `targettype`: Target type for coercion (InvalidOid if no specific target)
- `isarg`: Boolean indicating if this is a PASSING argument (affects type handling)

## Dependencies
- Functions called/Symbols referenced:
  - [transformExprRecurse](transformExprRecurse.md) (to transform raw expression)
  - exprType, exprLocation (expression utilities)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md), coerce_to_target_type (type coercion)
  - [get_type_category_preferred](../g/get_type_category_preferred.md) (type category analysis)
  - [makeJsonByteaToTextConversion](../m/makeJsonByteaToTextConversion.md) (bytea-to-text conversion)
  - makeFuncExpr (function expression creation)
  - copyObject (object copying)
  - Various type OIDs and constants (JSONOID, JSONBOID, BYTEAOID, etc.)
  - JSON format constants (JS_FORMAT_DEFAULT, JS_FORMAT_JSON, JS_FORMAT_JSONB)
  - Error reporting functions (ereport, errcode, errmsg, parser_errposition)
- Called from (representative examples):
  - [transformJsonObjectConstructor](transformJsonObjectConstructor.md)
  - [transformJsonObjectAgg](transformJsonObjectAgg.md)
  - [transformJsonArrayAgg](transformJsonArrayAgg.md)
  - [transformJsonArrayConstructor](transformJsonArrayConstructor.md)
  - [transformJsonParseExpr](transformJsonParseExpr.md)
  - [transformJsonSerializeExpr](transformJsonSerializeExpr.md)
  - [transformJsonFuncExpr](transformJsonFuncExpr.md)
  - [transformJsonPassingArgs](transformJsonPassingArgs.md)

## Notes and Other Information
- This is a static helper function within parse_expr.c
- Implements complex logic for JSON format handling and type coercion
- Special handling for PASSING arguments allows direct passthrough of supported types
- Encoding specifications are only valid for bytea input types
- The function can return either a simple expression or a JsonValueExpr wrapper
- Extensive validation ensures proper error reporting for invalid format/type combinations
- Uses assertion to verify that returned JsonValueExpr nodes have formatted_expr set
- Critical component of PostgreSQL's JSON processing infrastructure