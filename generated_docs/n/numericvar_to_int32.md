# numericvar_to_int32

## Location
[src/backend/utils/adt/numeric.c:4476-4492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4476-L4492)

## Overview
Converts a NumericVar to a 32-bit signed integer with range validation, returning false if the value exceeds int32 bounds.

## Definition

```c
static bool
numericvar_to_int32(const NumericVar *var, int32 *result)
```
## Detailed Description
This function performs a safe conversion from PostgreSQL's internal numeric representation (NumericVar) to a 32-bit signed integer. It first converts the NumericVar to a 64-bit integer using , then validates that the resulting value falls within the valid range for int32 (PG_INT32_MIN to PG_INT32_MAX). If the conversion is successful and the value is within range, it stores the result in the provided output parameter and returns true. The function is designed to prevent overflow errors during numeric-to-integer conversions.

## Parameters / Member Variables
- : Pointer to the input NumericVar structure to be converted (not modified by this function)
- : Pointer to int32 where the converted value will be stored if successful

## Dependencies
- Functions called/Symbols referenced:
  - [numericvar_to_int64](numericvar_to_int64.md)
  - PG_INT32_MIN
  - PG_INT32_MAX
- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT
  - [width_bucket_numeric](../w/width_bucket_numeric.md)
  - [numeric_int4_opt_error](numeric_int4_opt_error.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within the numeric.c source file
- Returns false on conversion failure or range overflow, true on success
- The input NumericVar is explicitly not freed by this function, leaving memory management to the caller
- Uses unlikely() macro hints for the range check conditions to optimize for the common case where values are within range
- Part of PostgreSQL's numeric type conversion infrastructure