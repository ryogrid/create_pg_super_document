# varbit_in

## Location
[src/backend/utils/adt/varbit.c:452-586](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L452-L586)

## Overview
Converts a string representation of a bit string (binary or hexadecimal format) to the internal VarBit data structure for variable-length bit strings.

## Definition

```c
structure */
				bitlen,			/* Number of bits in the bit string   */
				slen;
```
## Detailed Description
The  function is a PostgreSQL built-in input function for the  (variable-length bit string) data type. It parses string representations of bit strings in either binary format (prefixed with 'b' or 'B') or hexadecimal format (prefixed with 'x' or 'X') and converts them into PostgreSQL's internal VarBit representation.

The function supports three input formats:
1. **Binary format**:  or  - each character represents one bit
2. **Hexadecimal format**:  or  - each character represents 4 bits  
3. **Implicit binary**:  - assumes binary format without prefix

Unlike the  function,  treats the type modifier () as the maximum allowed length rather than the exact required length, allowing for variable-length bit strings up to the specified limit.

## Parameters / Member Variables
-  (char*): The input string containing the bit string representation
-  (Oid): Element type OID (unused, legacy parameter)
-  (int32): Maximum allowed bit length for this varbit type, or -1 if unspecified
-  (Node*): Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (extract input string argument)
  - PG_GETARG_INT32 (extract type modifier)
  - strlen (calculate string length)
  - VARBITMAXLEN (maximum allowed bit string length constant)
  - ereturn (soft error return with context)
  - VARBITTOTALLEN (calculate total allocation size for bit string)
  - [palloc0](../p/palloc0.md) (allocate zero-initialized memory)
  - SET_VARSIZE (set PostgreSQL variable-size header)
  - VARBITLEN (set bit string length)
  - VARBITS (get pointer to bit data)
  - HIGHBIT (high-order bit constant for parsing)
  - [pg_mblen](../p/pg_mblen.md) (multibyte character length for error reporting)
  - PG_RETURN_VARBIT_P (return VarBit result)
- Called from (representative examples):
  - PostgreSQL's type system during input conversion
  - SQL operations involving varbit literals or casts
  - COPY operations importing varbit data

## Notes and Other Information
- Supports both binary () and hexadecimal () input formats
- Validates that input characters are appropriate for the chosen format (0/1 for binary, 0-9/A-F/a-f for hex)
- Enforces maximum length constraints when type modifier is specified
- Uses soft error handling (ereturn) when error context is available for better error reporting
- Automatically zero-pads the result to ensure proper bit string format
- For hex format, validates that the total bit length doesn't exceed VARBITMAXLEN/4 characters
- Located in src/backend/utils/adt/varbit.c:452-586
- Part of PostgreSQL's comprehensive bit string type system alongside bit_in, bit_out, and related functions

## Simplified Source

```c
Datum varbit_in(PG_FUNCTION_ARGS) {
    char *input_string = PG_GETARG_CSTRING(0);
    int32 atttypmod = PG_GETARG_INT32(2);
    Node *escontext = fcinfo->context;

    // Determine input format (binary vs hex)
    char *sp;
    bool bit_not_hex;
    if (input_string[0] == 'b' || input_string[0] == 'B') {
        bit_not_hex = true;
        sp = input_string + 1;
    } else if (input_string[0] == 'x' || input_string[0] == 'X') {
        bit_not_hex = false;
        sp = input_string + 1;
    } else {
        bit_not_hex = true;
        sp = input_string;
    }

    // Calculate bit length from input
    int slen = strlen(sp);
    int bitlen = bit_not_hex ? slen : slen * 4;

    // Validate against hex length limits
    if (!bit_not_hex && slen > VARBITMAXLEN / 4)
        ereturn(escontext, (Datum) 0, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("bit string length exceeds the maximum allowed (%d)", VARBITMAXLEN)));

    // Check type modifier constraints
    if (atttypmod <= 0)
        atttypmod = bitlen;
    else if (bitlen > atttypmod)
        ereturn(escontext, (Datum) 0, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                errmsg("bit string too long for type bit varying(%d)", atttypmod)));

    // Allocate and initialize result
    int len = VARBITTOTALLEN(bitlen);
    VarBit *result = (VarBit *) palloc0(len);
    SET_VARSIZE(result, len);
    VARBITLEN(result) = Min(bitlen, atttypmod);

    // Parse input into bit data
    bits8 *r = VARBITS(result);
    if (bit_not_hex) {
        // Parse binary format
        bits8 x = HIGHBIT;
        for (; *sp; sp++) {
            if (*sp == '1')
                *r |= x;
            else if (*sp != '0')
                ereturn(escontext, (Datum) 0, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("\"%.*s\" is not a valid binary digit", pg_mblen(sp), sp)));

            x >>= 1;
            if (x == 0) {
                x = HIGHBIT;
                r++;
            }
        }
    } else {
        // Parse hexadecimal format
        int bc = 0;
        for (; *sp; sp++) {
            bits8 x;
            if (*sp >= '0' && *sp <= '9')
                x = (bits8) (*sp - '0');
            else if (*sp >= 'A' && *sp <= 'F')
                x = (bits8) (*sp - 'A') + 10;
            else if (*sp >= 'a' && *sp <= 'f')
                x = (bits8) (*sp - 'a') + 10;
            else
                ereturn(escontext, (Datum) 0, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("\"%.*s\" is not a valid hexadecimal digit", pg_mblen(sp), sp)));

            if (bc) {
                *r++ |= x;
                bc = 0;
            } else {
                *r = x << 4;
                bc = 1;
            }
        }
    }

    PG_RETURN_VARBIT_P(result);
}
```