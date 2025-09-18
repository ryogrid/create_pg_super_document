# makeJsonByteaToTextConversion

## Location
src/backend/parser/parse_expr.c: 3266 - 3287

## Overview
Creates a function expression that converts bytea data to text using a specified JSON format encoding.

## Definition
```c
static Node *makeJsonByteaToTextConversion(Node *expr, JsonFormat *format, int location)
```

## Detailed Description
This static helper function constructs a FuncExpr node that represents a call to the convert_from() function, which converts binary data (bytea) to text using a specified character encoding. The function is specifically designed for JSON processing contexts where bytea data needs to be converted to text with proper encoding handling.

The function creates a two-argument function call where the first argument is the bytea expression to be converted and the second argument is a constant containing the encoding name (obtained from the JsonFormat). The conversion is marked as an explicit coercion call, indicating that this is an intentional type conversion rather than an implicit cast.

## Parameters / Member Variables
- `expr`: The bytea expression node to be converted to text
- `format`: JsonFormat structure specifying the target encoding (can be NULL for default UTF8)
- `location`: Source location information for error reporting and debugging

## Dependencies
- Functions called/Symbols referenced:
  - getJsonEncodingConst (to get encoding constant)
  - makeFuncExpr (to create function expression node)
  - list_make2 (to create argument list)
  - JsonFormat (input parameter type)
  - FuncExpr (created node type)  
  - Const (for encoding argument)
  - F_CONVERT_FROM (function OID constant)
  - TEXTOID (target type OID)
  - InvalidOid (for missing OID values)
  - COERCE_EXPLICIT_CALL (coercion type constant)
- Called from (representative examples):
  - transformJsonValueExpr
  - transformJsonParseArg

## Notes and Other Information
- This is a static helper function within parse_expr.c
- Used specifically in JSON processing contexts for bytea-to-text conversion
- Creates an explicit function call to convert_from() with proper encoding
- Location information is preserved for error reporting
- The function handles default encoding through getJsonEncodingConst()
- Part of PostgreSQL's JSON processing infrastructure for handling binary data conversion