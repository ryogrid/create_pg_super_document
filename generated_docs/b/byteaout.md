# byteaout

## Location
[src/backend/utils/adt/varlena.c:388-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L388-L470)

## Overview
Converts bytea (binary data array) to its printable string representation for output. Supports both hexadecimal and traditional escaped output formats based on the bytea_output configuration setting.

## Definition

```c
Datum
byteaout(PG_FUNCTION_ARGS)
```
## Detailed Description
The byteaout function is responsible for converting PostgreSQL's internal bytea representation to a human-readable string format. It operates in two distinct modes:

1. **Hexadecimal format (BYTEA_OUTPUT_HEX)**: Produces output prefixed with '\x' followed by hexadecimal digit pairs representing each byte.

2. **Escaped format (BYTEA_OUTPUT_ESCAPE)**: Uses traditional C-style escape sequences where non-printable characters (< 0x20 or > 0x7e) are represented as octal escapes '\nnn' and backslashes are doubled as '\\'.

The function calculates the required output buffer size first to prevent buffer overflows, with safety checks against MaxAllocSize to prevent memory exhaustion attacks.

## Parameters / Member Variables
- Input: bytea value retrieved via  - the binary data to be converted
- Returns: C-string representation via  

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP
  - [hex_encode](../h/hex_encode.md)
  - [palloc](../p/palloc.md)
  - ereport/elog
  - VARSIZE_ANY_EXHDR
  - VARDATA_ANY
  - DIG (digit conversion macro)
  - PG_RETURN_CSTRING
- Constants referenced:
  - BYTEA_OUTPUT_HEX
  - BYTEA_OUTPUT_ESCAPE
  - MaxAllocSize
- Called from:
  - [pg_mcv_list_out](../p/pg_mcv_list_out.md) (statistics module)

## Notes and Other Information
- The function respects the global bytea_output setting to determine output format
- Includes overflow protection by checking against MaxAllocSize before allocation
- Uses efficient single-pass algorithms for both format conversions
- The escaped format calculation is done in two passes: first to calculate required size, then to perform the actual conversion
- Non-printable character detection uses the standard ASCII printable range (0x20-0x7e)

## Simplified Source

```c
Datum byteaout(PG_FUNCTION_ARGS) {
    bytea *vlena = PG_GETARG_BYTEA_PP(0);
    char *result;
    char *rp;

    if (bytea_output == BYTEA_OUTPUT_HEX) {
        // Hex format: \x followed by hex digits
        rp = result = palloc(VARSIZE_ANY_EXHDR(vlena) * 2 + 2 + 1);
        *rp++ = '\\';
        *rp++ = 'x';
        rp += hex_encode(VARDATA_ANY(vlena), VARSIZE_ANY_EXHDR(vlena), rp);
    }
    else if (bytea_output == BYTEA_OUTPUT_ESCAPE) {
        // Traditional escaped format
        char *vp = VARDATA_ANY(vlena);
        uint64 len = 1; // Start with 1 for empty string

        // Calculate required output length
        for (int i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++) {
            if (*vp == '\\')
                len += 2;  // Escape backslash as \\
            else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
                len += 4;  // Non-printable as \nnn
            else
                len++;     // Printable as-is
        }

        // Check for overflow protection
        if (len > MaxAllocSize)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                          errmsg_internal("result of bytea output conversion is too large")));

        // Perform the actual conversion
        char *rp = result = palloc(len);
        vp = VARDATA_ANY(vlena);

        for (int i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++) {
            if (*vp == '\\') {
                *rp++ = '\\'; *rp++ = '\\';
            }
            else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e) {
                // Convert to octal escape sequence
                int val = *vp;
                rp[0] = '\\';
                rp[3] = DIG(val & 07); val >>= 3;
                rp[2] = DIG(val & 07); val >>= 3;
                rp[1] = DIG(val & 03);
                rp += 4;
            }
            else {
                *rp++ = *vp;
            }
        }
    }
    else {
        elog(ERROR, "unrecognized \"bytea_output\" setting: %d", bytea_output);
        result = NULL; // Keep compiler quiet
    }

    *rp = '\0';
    return PG_RETURN_CSTRING(result);
}
```