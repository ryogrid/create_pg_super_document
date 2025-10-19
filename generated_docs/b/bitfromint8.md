# bitfromint8

## Location
[src/backend/utils/adt/varbit.c:1611-1665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1611-L1665)

## Overview
Converts a 64-bit integer to a variable-length bit string representation, preserving the natural bit ordering.

## Definition

```c
Datum
bitfromint8(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function converts a 64-bit signed integer to a PostgreSQL variable-length bit string (varbit) with a specified length. This function is very similar to  but operates on 64-bit integers instead of 32-bit. It preserves the natural ordering of bits and handles sign extension for negative numbers. It supports creating bit strings of any length up to the maximum allowed, truncating or sign-extending the input integer as needed.

Key behaviors:
- Preserves natural bit ordering (most significant bit first)
- Performs sign extension for negative integers when the target length exceeds 64 bits
- Truncates input bits if the target length is less than 64 bits
- Uses typmod parameter to specify the desired bit string length
- Defaults to 1-bit length if typmod is invalid
- Handles the full 64-bit range of input values

## Parameters / Member Variables
- : The 64-bit signed integer to convert (int64)
- : The desired length of the resulting bit string (int32)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (extract 64-bit integer argument)
  - PG_GETARG_INT32 (extract integer argument for typmod)
  - VARBITTOTALLEN (calculate total storage length)
  - [palloc](../p/palloc.md) (allocate memory for result)
  - SET_VARSIZE (set variable-length object size)
  - VARBITLEN (set bit string length)
  - VARBITS (access bit data)
  - BITMASK (bit masking constant)
  - Min (minimum value macro)
  - PG_RETURN_VARBIT_P (return result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Implements PostgreSQL's 64-bit integer to bit string conversion functionality
- The function is not defined in any SQL standard but follows intuitive bit ordering
- Handles both positive and negative integers with proper sign extension
- For negative numbers, performs arithmetic right shift with sign fill
- The implementation carefully handles fractional bytes at both ends of the conversion
- Maximum bit string length is bounded by VARBITMAXLEN
- Used internally by PostgreSQL's type system for casting operations from bigint to bit
- Provides extended range compared to  for larger integer values

## Simplified Source

```c
Datum
bitfromint8(PG_FUNCTION_ARGS)
{
    int64 a = PG_GETARG_INT64(0);
    int32 typmod = PG_GETARG_INT32(1);
    VarBit *result;
    bits8 *r;
    int rlen;
    int destbitsleft, srcbitsleft;

    // Use default length of 1 if typmod is invalid
    if (typmod <= 0 || typmod > VARBITMAXLEN)
        typmod = 1;

    // Allocate result bit string
    rlen = VARBITTOTALLEN(typmod);
    result = (VarBit *) palloc(rlen);
    SET_VARSIZE(result, rlen);
    VARBITLEN(result) = typmod;

    r = VARBITS(result);
    destbitsleft = typmod;
    srcbitsleft = 64;

    // Limit source bits to destination size
    srcbitsleft = Min(srcbitsleft, destbitsleft);

    // Sign-fill any excess bytes in output
    while (destbitsleft >= srcbitsleft + 8) {
        *r++ = (bits8) ((a < 0) ? BITMASK : 0);
        destbitsleft -= 8;
    }

    // Store first fractional byte if needed
    if (destbitsleft > srcbitsleft) {
        unsigned int val = (unsigned int) (a >> (destbitsleft - 8));
        // Force sign-fill for negative numbers
        if (a < 0)
            val |= ((unsigned int) -1) << (srcbitsleft + 8 - destbitsleft);
        *r++ = (bits8) (val & BITMASK);
        destbitsleft -= 8;
    }

    // Store whole bytes
    while (destbitsleft >= 8) {
        *r++ = (bits8) ((a >> (destbitsleft - 8)) & BITMASK);
        destbitsleft -= 8;
    }

    // Store last fractional byte
    if (destbitsleft > 0)
        *r = (bits8) ((a << (8 - destbitsleft)) & BITMASK);

    return result;
}
```