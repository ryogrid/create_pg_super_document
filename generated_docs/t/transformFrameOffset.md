# transformFrameOffset

## Location
src/backend/parser/parse_clause.c: 3686 - 3822

## Overview
Processes a window frame offset expression for SQL window functions, handling type coercion and validation for ROWS, RANGE, and GROUPS frame modes.

## Definition
```c
static Node *
transformFrameOffset(ParseState *pstate, int frameOptions,
                     Oid rangeopfamily, Oid rangeopcintype, Oid *inRangeFunc,
                     Node *clause)
```

## Detailed Description
This function transforms and validates offset expressions used in window frame specifications. It handles three different frame modes:

1. **ROWS mode**: Coerces the offset expression to `int8` (bigint), treating it as a row count
2. **RANGE mode**: Performs complex type resolution to find appropriate `in_range` support functions from the sort operator family, ensuring type compatibility between the offset expression and the ORDER BY column
3. **GROUPS mode**: Similar to ROWS, coerces to `int8` but operates on groups of equal values

For RANGE mode, the function searches the system catalog for `in_range` support functions that match the sort operator family and can accept the offset expression type. It handles multiple candidate functions by preferring exact type matches and providing detailed error messages when no suitable function is found.

The function also validates that the offset expression contains no variables, ensuring it evaluates to a constant value.

## Parameters / Member Variables
- `pstate`: Parser state containing context information for error reporting and expression transformation
- `frameOptions`: Bitmask indicating the frame mode (FRAMEOPTION_ROWS, FRAMEOPTION_RANGE, or FRAMEOPTION_GROUPS)
- `rangeopfamily`: OID of the sort operator family for the ORDER BY column (used in RANGE mode)
- `rangeopcintype`: Input data type that the sort operator is registered with (used in RANGE mode)  
- `inRangeFunc`: Output parameter that receives the OID of the selected in_range function (RANGE mode only)
- `clause`: The raw offset expression node to be transformed, can be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](transformExpr.md)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md)
  - exprType
  - SearchSysCacheList2
  - [can_coerce_type](../c/can_coerce_type.md)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md)
  - [checkExprIsVarFree](../c/checkExprIsVarFree.md)
  - [exprLocation](../e/exprLocation.md)
  - [format_type_be](../f/format_type_be.md)
  - ereport
- Called from (representative examples):
  - [transformWindowDefinitions](transformWindowDefinitions.md)

## Notes and Other Information
- Returns NULL if the clause parameter is NULL (no offset specified)
- For RANGE mode, the function implements sophisticated type resolution logic to handle cases where multiple in_range functions might be applicable
- Error messages provide specific guidance about type casting when automatic coercion fails
- The function enforces that frame offsets must be constant expressions (no variables allowed)
- Located in src/backend/parser/parse_clause.c:3686-3822