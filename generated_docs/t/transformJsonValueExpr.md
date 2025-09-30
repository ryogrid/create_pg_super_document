# transformJsonValueExpr

## Location
[src/backend/parser/parse_expr.c:3288-3449](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3288-L3449)

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
  - [exprType](../e/exprType.md), exprLocation (expression utilities)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md), coerce_to_target_type (type coercion)
  - [get_type_category_preferred](../g/get_type_category_preferred.md) (type category analysis)
  - [makeJsonByteaToTextConversion](../m/makeJsonByteaToTextConversion.md) (bytea-to-text conversion)
  - [makeFuncExpr](../m/makeFuncExpr.md) (function expression creation)
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

## Simplified Source

```c
static Node *
transformJsonValueExpr(ParseState *pstate, const char *constructName,
                      JsonValueExpr *ve, JsonFormatType default_format,
                      Oid targettype, bool isarg) {
    Node *expr = transformExprRecurse(pstate, (Node *) ve->raw_expr);
    Node *rawexpr;
    JsonFormatType format;
    Oid exprtype;
    int location;
    char typcategory;
    bool typispreferred;

    // Handle unknown type by coercing to text
    if (exprType(expr) == UNKNOWNOID) {
        expr = coerce_to_specific_type(pstate, expr, TEXTOID, constructName);
    }

    rawexpr = expr;
    exprtype = exprType(expr);
    location = exprLocation(expr);
    get_type_category_preferred(exprtype, &typcategory, &typispreferred);

    // Determine format based on specifications and context
    if (ve->format->format_type != JS_FORMAT_DEFAULT) {
        // Validate encoding is only for bytea
        if (ve->format->encoding != JS_ENC_DEFAULT && exprtype != BYTEAOID) {
            ereport(ERROR, "JSON ENCODING clause only allowed for bytea input");
        }

        // Don't format existing JSON types
        if (exprtype == JSONOID || exprtype == JSONBOID) {
            format = JS_FORMAT_DEFAULT;
        } else {
            format = ve->format->format_type;
        }
    } else if (isarg) {
        // Special handling for PASSING arguments - pass supported types directly
        switch (exprtype) {
            case BOOLOID:
            case NUMERICOID:
            case INT2OID: case INT4OID: case INT8OID:
            case FLOAT4OID: case FLOAT8OID:
            case TEXTOID: case VARCHAROID:
            case DATEOID: case TIMEOID: case TIMETZOID:
            case TIMESTAMPOID: case TIMESTAMPTZOID:
                return expr;  // Pass through directly
            default:
                if (typcategory == TYPCATEGORY_STRING) {
                    return expr;
                }
                // Convert to JSON for other types
                break;
        }
        format = default_format;
    } else {
        // Don't format existing JSON types, use default for others
        format = (exprtype == JSONOID || exprtype == JSONBOID)
                 ? JS_FORMAT_DEFAULT : default_format;
    }

    // Apply formatting and coercion if needed
    if (format != JS_FORMAT_DEFAULT ||
        (OidIsValid(targettype) && exprtype != targettype)) {

        Node *coerced;
        bool only_allow_cast = OidIsValid(targettype);

        // Validate non-string types with format restrictions
        if (!isarg && !only_allow_cast &&
            exprtype != BYTEAOID && typcategory != TYPCATEGORY_STRING) {
            ereport(ERROR, "cannot use non-string types with FORMAT JSON clause");
        }

        // Handle bytea to text conversion for JSON
        if (format == JS_FORMAT_JSON && exprtype == BYTEAOID) {
            expr = makeJsonByteaToTextConversion(expr, ve->format, location);
            exprtype = TEXTOID;
        }

        // Set target type if not specified
        if (!OidIsValid(targettype)) {
            targettype = (format == JS_FORMAT_JSONB) ? JSONBOID : JSONOID;
        }

        // Try direct coercion first
        coerced = coerce_to_target_type(pstate, expr, exprtype, targettype, -1,
                                       COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
                                       location);

        if (!coerced) {
            // Fall back to to_json()/to_jsonb() functions
            if (only_allow_cast) {
                ereport(ERROR, "cannot cast type to target type");
            }

            Oid fnoid = (targettype == JSONOID) ? F_TO_JSON : F_TO_JSONB;
            FuncExpr *fexpr = makeFuncExpr(fnoid, targettype, list_make1(expr),
                                          InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
            fexpr->location = location;
            coerced = (Node *) fexpr;
        }

        // Return appropriate result
        if (coerced == expr) {
            expr = rawexpr;
        } else {
            // Create JsonValueExpr with both raw and formatted expressions
            ve = copyObject(ve);
            ve->raw_expr = (Expr *) rawexpr;
            ve->formatted_expr = (Expr *) coerced;
            expr = (Node *) ve;
        }
    }

    return expr;
}
```