# varbit_out

## Location
src/backend/utils/adt/varbit.c: 587 - 635

## Overview
Converts a variable-length bit string (VarBit) to its external string representation as a sequence of '0' and '1' characters.

## Definition


## Detailed Description
The  function is responsible for converting PostgreSQL's internal VarBit representation to a human-readable string format consisting of '0' and '1' characters. This function preserves the exact length of the bit string, unlike some other representations that might lose precision. The function processes the bit string byte by byte, extracting individual bits and converting them to their character representation.

The function includes an assertion check  to ensure the input bit string is properly padded, which helps catch potential issues in other bit manipulation functions. For each byte, it processes all 8 bits using bit shifting operations to extract individual bits. For the final partial byte (if the bit length is not a multiple of 8), it processes only the relevant bits.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Input VarBit pointer obtained via 

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P
  - VARBIT_CORRECTLY_PADDED
  - VARBITLEN
  - VARBITS
  - IS_HIGHBIT_SET
  - PG_RETURN_CSTRING
  - BITS_PER_BYTE
- Called from:
  - [bit_out](../b/bit_out.md)

## Notes and Other Information
- The function includes a comment noting that  and hex input to  can load values that this function cannot emit, suggesting potential for hex output format for such values
- Uses bit shifting operations () to efficiently iterate through bits within each byte
- Allocates result string with  to accommodate the null terminator
- The output format is purely binary (0s and 1s), making it easy to understand the exact bit pattern
- Located in 