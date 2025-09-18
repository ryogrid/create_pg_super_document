# bit_in

## Location
src/backend/utils/adt/varbit.c: 147 - 279

## Overview
Converts a string representation (binary, hexadecimal, or plain binary) to PostgreSQL's internal bit string representation, handling type modifier validation and various input formats.

## Definition
```c
Datum bit_in(PG_FUNCTION_ARGS)
```

## Detailed Description
The bit_in function is PostgreSQL's input function for the BIT data type. It takes a string representation of a bit sequence and converts it into the internal VarBit format used by PostgreSQL. The function supports three input formats:

1. **Binary format with 'b' prefix**: e.g., "b1010" or "B1010"
2. **Hexadecimal format with 'x' prefix**: e.g., "xA5" or "XA5" 
3. **Plain binary format**: e.g., "1010" (interpreted as binary)

The function performs comprehensive validation including:
- Input format validation (binary digits 0/1 or hex digits 0-9, A-F, a-f)
- Length constraint enforcement based on the type modifier (atttypmod)
- Maximum length validation to prevent excessive memory allocation
- Proper error reporting with context-sensitive messages

For hexadecimal input, each hex digit represents 4 bits. For binary input, each character represents 1 bit. The resulting internal representation is packed into bytes with proper bit alignment.

## Parameters / Member Variables
- `input_string`: The string representation to convert (from PG_GETARG_CSTRING(0))
- `typelem`: Element type OID (unused, from PG_GETARG_OID(1))  
- `atttypmod`: Type modifier specifying expected bit length (from PG_GETARG_INT32(2))
- `escontext`: Error context for soft error reporting

## Dependencies
- Functions called/Symbols referenced:
  - VarBit, bits8 (type definitions)
  - VARBITMAXLEN (maximum bit string length constant)
  - VARBITTOTALLEN (macro for calculating storage size)
  - SET_VARSIZE, VARBITLEN, VARBITS (VarBit manipulation macros)
  - HIGHBIT (bit manipulation constant)
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - ereturn (soft error return)
  - [pg_mblen](../p/pg_mblen.md) (multibyte character length)
  - PG_RETURN_VARBIT_P (return macro)
- Called from (representative examples):
  - [make_const](../m/make_const.md) (during constant parsing)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via the type system
- Supports soft error reporting through the escontext parameter for graceful error handling
- Uses bit-packing techniques to efficiently store bit sequences in byte arrays
- Handles both fixed-length BIT and variable-length VARBIT through the same implementation
- Input validation ensures data integrity and prevents buffer overflows
- Zero-padding ensures consistent internal representation regardless of input format
- The function automatically detects input format based on prefix characters ('b'/'B' or 'x'/'X')