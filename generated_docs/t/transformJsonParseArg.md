# transformJsonParseArg

## Location
src/backend/parser/parse_expr.c: 4040 - 4089

## Overview
Prepares and transforms a JSON document argument for JSON parsing operations, handling type coercion and format encoding.

## Definition
```c
static Node *transformJsonParseArg(ParseState *pstate, Node *jsexpr, JsonFormat *format, Oid *exprtype)
```

## Detailed Description
This function processes JSON document arguments used in JSON parsing operations. It handles type coercion from various input types to appropriate formats for JSON processing. For BYTEA inputs, it performs conversion to text and wraps the result in a JsonValueExpr. For string-category types, it coerces to TEXT type. The function also validates format encoding restrictions, ensuring encoding clauses are only used with BYTEA input types.

## Parameters / Member Variables
- `pstate`: ParseState pointer containing parsing context and state information
- `jsexpr`: Node pointer to the expression representing the JSON document to be parsed
- `format`: JsonFormat pointer specifying format options including encoding
- `exprtype`: Oid pointer (output parameter) that receives the final expression type after transformations

## Dependencies
- Functions called/Symbols referenced:
  - [transformExprRecurse](transformExprRecurse.md)
  - exprType
  - [makeJsonByteaToTextConversion](../m/makeJsonByteaToTextConversion.md)
  - [makeJsonValueExpr](../m/makeJsonValueExpr.md)
  - [get_type_category_preferred](../g/get_type_category_preferred.md)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - [exprLocation](../e/exprLocation.md)
  - ereport
- Called from (representative examples):
  - [transformJsonIsPredicate](transformJsonIsPredicate.md) (src/backend/parser/parse_expr.c:4093)
  - [transformJsonParseExpr](transformJsonParseExpr.md) (src/backend/parser/parse_expr.c:4170)

## Notes and Other Information
- Handles special case for BYTEAOID inputs by converting to text format
- Validates that JSON FORMAT ENCODING clauses are only used with bytea input types
- Performs implicit type coercion for string-category and unknown types to TEXTOID
- Part of the JSON parsing infrastructure that ensures consistent input formatting
- Error reporting includes parser location information for better diagnostics