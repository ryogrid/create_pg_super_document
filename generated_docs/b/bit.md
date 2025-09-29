# bit

## Location
[src/backend/utils/adt/varbit.c:391-428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L391-L428)

## Overview
Converts a variable-length bit string to a fixed-length bit string with a specified length, handling both explicit and implicit type casts with appropriate validation and padding.

## Definition

```c
Datum
bit(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that performs length conversion for bit strings, specifically converting from variable-length bit strings () to fixed-length bit strings () with a specified target length. This function handles both explicit and implicit type casts differently:

- **Implicit casts**: Raises an error if the source data length doesn't match the target length exactly
- **Explicit casts**: Silently truncates or zero-pads the source data to match the target length

The function ensures proper zero-padding for shorter inputs and truncation for longer inputs when performing explicit casts. It also validates that the target length is within acceptable bounds (1 to VARBITMAXLEN).

## Parameters / Member Variables
-  (VarBit*): The input bit string to be converted
-  (int32): The target bit length specified in the column definition or cast
-  (bool): Flag indicating whether this is an explicit cast (true) or implicit cast (false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P (extract bit string argument)
  - PG_GETARG_INT32 (extract length argument)  
  - PG_GETARG_BOOL (extract explicit cast flag)
  - VARBITLEN (get bit string length)
  - VARBITMAXLEN (maximum allowed bit string length)
  - VARBITTOTALLEN (calculate total allocation size)
  - [palloc0](../p/palloc0.md) (allocate zero-initialized memory)
  - SET_VARSIZE (set variable-size header)
  - VARBITS (get pointer to bit data)
  - VARBITBYTES (get byte count for bit data)
  - VARBIT_PAD (ensure proper zero-padding)
  - PG_RETURN_VARBIT_P (return result)
- Called from (representative examples):
  - Type casting operations in SQL queries
  - Column definition enforcement during INSERT/UPDATE
  - PostgreSQL's type coercion system

## Notes and Other Information
- Returns the original input unchanged if target length is invalid or matches current length
- For explicit casts, truncates excess bits or zero-pads shorter bit strings to match target length
- For implicit casts, throws STRING_DATA_LENGTH_MISMATCH error if lengths don't match exactly
- Ensures the last byte of the result is properly zero-padded to maintain bit string integrity
- Part of PostgreSQL's comprehensive type system for handling bit string data types
- Located in src/backend/utils/adt/varbit.c:391-428

## Simplified Source

```c
Datum bit(PG_FUNCTION_ARGS) {
    VarBit *arg = PG_GETARG_VARBIT_P(0);
    int32 len = PG_GETARG_INT32(1);
    bool isExplicit = PG_GETARG_BOOL(2);
    VarBit *result;
    int rlen;

    // Return unchanged if target length is invalid or already matches
    if (len <= 0 || len > VARBITMAXLEN || len == VARBITLEN(arg)) {
        PG_RETURN_VARBIT_P(arg);
    }

    // For implicit casts, require exact length match
    if (!isExplicit) {
        ereport(ERROR,
                (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
                 errmsg("bit string length %d does not match type bit(%d)",
                        VARBITLEN(arg), len)));
    }

    // Allocate result with target length (zero-initialized)
    rlen = VARBITTOTALLEN(len);
    result = (VarBit *) palloc0(rlen);
    SET_VARSIZE(result, rlen);
    VARBITLEN(result) = len;

    // Copy data (truncating or padding as needed)
    memcpy(VARBITS(result), VARBITS(arg),
           Min(VARBITBYTES(result), VARBITBYTES(arg)));

    // Ensure proper zero-padding in last byte
    VARBIT_PAD(result);

    PG_RETURN_VARBIT_P(result);
}
```