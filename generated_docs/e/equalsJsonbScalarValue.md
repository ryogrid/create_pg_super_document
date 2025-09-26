# equalsJsonbScalarValue

## Location
[src/backend/utils/adt/jsonb_util.c:1407-1438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1407-L1438)

## Overview
equalsJsonbScalarValue determines whether two JsonbValue scalar values of the same type are equal, implementing type-specific equality comparison logic.

## Definition

```c
static bool
equalsJsonbScalarValue(JsonbValue *a, JsonbValue *b)
```
## Detailed Description
This static function performs equality comparison between two JsonbValue scalar values, requiring that both values have the same type before comparison. It implements type-specific equality logic: null values are always equal to other nulls, string values are compared using lengthCompareJsonbStringValue, numeric values use PostgreSQL's numeric_eq function to ensure mathematical equality, and boolean values are compared directly. The function enforces type safety by generating errors for mismatched types or invalid scalar types, making it suitable for use in JSONB containment and search operations.

## Parameters / Member Variables
- : Pointer to the first JsonbValue scalar for comparison
- : Pointer to the second JsonbValue scalar for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [lengthCompareJsonbStringValue](../l/lengthCompareJsonbStringValue.md) (for string comparison)
  - DirectFunctionCall2 (for calling numeric_eq)
  - [numeric_eq](../n/numeric_eq.md) (for numeric equality comparison)
  - [PointerGetDatum](../P/PointerGetDatum.md) (for datum conversion)
  - [DatumGetBool](../D/DatumGetBool.md) (for boolean result conversion)
- Called from (representative examples):
  - [findJsonbValueFromContainer](../f/findJsonbValueFromContainer.md)
  - [JsonbDeepContains](../J/JsonbDeepContains.md)

## Notes and Other Information
The function is declared static, limiting its scope to the jsonb_util.c file. It requires both values to have identical types and will generate ERROR conditions for type mismatches or invalid scalar types. This strict type checking ensures that equality comparisons are semantically meaningful and prevents undefined behavior. The function is a key component in JSONB containment checking and value lookup operations.