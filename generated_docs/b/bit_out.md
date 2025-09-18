# bit_out

## Location
src/backend/utils/adt/varbit.c: 280 - 330

## Overview
Converts PostgreSQL's internal bit string representation to its external string format, currently implemented as a wrapper around varbit_out for consistent output formatting.

## Definition
```c
Datum bit_out(PG_FUNCTION_ARGS)
```

## Detailed Description
The bit_out function is PostgreSQL's output function for the BIT data type. It converts the internal VarBit representation back to a human-readable string format. Currently, the function is implemented as a simple wrapper that delegates to varbit_out, ensuring consistent output formatting between BIT and VARBIT types.

The function includes commented-out alternative implementation code that demonstrates how to format bit strings as hexadecimal output with an 'X' prefix (e.g., "X1A2B"). This alternative approach:
1. Calculates the required output length based on bit length
2. Converts each 4-bit group to a hexadecimal digit
3. Handles partial byte boundaries correctly
4. Zero-pads the output appropriately

The current active implementation simply calls varbit_out(fcinfo), which produces binary string output (e.g., "1010") consistent with standard PostgreSQL bit string display conventions.

## Parameters / Member Variables
- Input parameter (via PG_FUNCTION_ARGS):
  - VarBit pointer: The internal bit string to convert to string format

## Dependencies
- Functions called/Symbols referenced:
  - [varbit_out](../v/varbit_out.md) (active implementation)
  - VarBit, bits8 (type definitions - commented code)
  - PG_GETARG_VARBIT_P, VARBIT_CORRECTLY_PADDED, VARBITLEN, VARBITS (VarBit macros - commented code)
  - HEXDIG (hex digit conversion macro - commented code)
  - [palloc](../p/palloc.md) (memory allocation - commented code)
  - PG_RETURN_CSTRING (return macro - commented code)
- Called from (representative examples):
  - Type system output functions (automatically invoked)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via the type system
- Currently delegates to varbit_out for unified output formatting between BIT and VARBIT types
- Contains alternative hexadecimal formatting implementation in commented code for potential future use
- The commented hex formatting code includes proper bit boundary handling and zero-padding logic
- Output format consistency ensures that BIT and VARBIT types display identically to users
- The function includes validation assertions (VARBIT_CORRECTLY_PADDED) to ensure internal bit string integrity
- Alternative implementations could be activated by changing the preprocessor conditional from '#if 1' to '#if 0'