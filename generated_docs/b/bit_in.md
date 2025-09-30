# bit_in

## Location
[src/backend/utils/adt/varbit.c:147-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L147-L279)

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

## Simplified Source

```c
Datum bit_in(PG_FUNCTION_ARGS) {
    char *input_string = PG_GETARG_CSTRING(0);
    int32 atttypmod = PG_GETARG_INT32(2);
    Node *escontext = fcinfo->context;

    char *sp;
    bool bit_not_hex;
    int bitlen, slen;

    // Determine input format based on prefix
    if (input_string[0] == 'b' || input_string[0] == 'B') {
        bit_not_hex = true;  // Binary format
        sp = input_string + 1;
    } else if (input_string[0] == 'x' || input_string[0] == 'X') {
        bit_not_hex = false;  // Hexadecimal format
        sp = input_string + 1;
    } else {
        bit_not_hex = true;  // Plain binary (no prefix)
        sp = input_string;
    }

    // Calculate bit length from input
    slen = strlen(sp);
    if (bit_not_hex) {
        bitlen = slen;  // One bit per character
    } else {
        // Check hex length limits
        if (slen > VARBITMAXLEN / 4) {
            ereturn(escontext, (Datum) 0, /* length error */);
        }
        bitlen = slen * 4;  // Four bits per hex character
    }

    // Validate against type modifier
    if (atttypmod <= 0) {
        atttypmod = bitlen;
    } else if (bitlen != atttypmod) {
        ereturn(escontext, (Datum) 0, /* length mismatch error */);
    }

    // Allocate result structure
    int len = VARBITTOTALLEN(atttypmod);
    VarBit *result = (VarBit *) palloc0(len);
    SET_VARSIZE(result, len);
    VARBITLEN(result) = atttypmod;

    bits8 *r = VARBITS(result);

    if (bit_not_hex) {
        // Parse binary input: '0' and '1' characters
        bits8 x = HIGHBIT;
        for (; *sp; sp++) {
            if (*sp == '1') {
                *r |= x;
            } else if (*sp != '0') {
                ereturn(escontext, (Datum) 0, /* invalid binary digit */);
            }

            x >>= 1;
            if (x == 0) {
                x = HIGHBIT;
                r++;
            }
        }
    } else {
        // Parse hexadecimal input: 0-9, A-F, a-f
        for (int bc = 0; *sp; sp++) {
            bits8 x;

            if (*sp >= '0' && *sp <= '9') {
                x = (bits8) (*sp - '0');
            } else if (*sp >= 'A' && *sp <= 'F') {
                x = (bits8) (*sp - 'A') + 10;
            } else if (*sp >= 'a' && *sp <= 'f') {
                x = (bits8) (*sp - 'a') + 10;
            } else {
                ereturn(escontext, (Datum) 0, /* invalid hex digit */);
            }

            if (bc) {
                *r++ |= x;  // Low nibble
                bc = 0;
            } else {
                *r = x << 4;  // High nibble
                bc = 1;
            }
        }
    }

    PG_RETURN_VARBIT_P(result);
}
```