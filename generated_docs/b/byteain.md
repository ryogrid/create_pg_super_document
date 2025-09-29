# byteain

## Location
[src/backend/utils/adt/varlena.c:290-387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L290-L387)

## Overview
PostgreSQL input function that converts printable text representations into internal bytea (byte array) format, supporting both hexadecimal and traditional escaped octal formats.

## Definition
```c
Datum byteain(PG_FUNCTION_ARGS)
```

## Detailed Description
The `byteain` function serves as PostgreSQL's input function for the bytea data type, responsible for parsing text representations of binary data and converting them into the internal bytea format. The function supports two distinct input formats:

1. **Hexadecimal format**: Strings beginning with '\x' followed by hexadecimal digits (e.g., '\x48656c6c6f' for "Hello")
2. **Traditional escaped format**: Uses octal escape sequences where non-printable characters are represented as '\nnn' (three octal digits) and literal backslashes as '\\\'

The function performs a two-pass algorithm: first pass validates the input and calculates the required output size, second pass performs the actual conversion. For hexadecimal input, it delegates to `hex_decode_safe` for efficient decoding. For escaped format, it manually processes each character, converting octal sequences to their byte values.

## Parameters / Member Variables
- Uses PostgreSQL's function call interface (`PG_FUNCTION_ARGS`)
- `inputText`: Input C string containing the text representation (`PG_GETARG_CSTRING(0)`)
- `escontext`: Error context for soft error handling (`fcinfo->context`)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING` - extracts C string argument from function call
  - `strlen` - calculates string length for hex format
  - `[palloc](../p/palloc.md)` - PostgreSQL memory allocation
  - `[hex_decode_safe](../h/hex_decode_safe.md)` - safe hexadecimal decoding with error context
  - `SET_VARSIZE` - sets the size field in variable-length header
  - `VARDATA` - gets pointer to data portion of variable-length type
  - `VARHDRSZ` - size of variable-length header
  - `VAL` - macro to convert octal digit to numeric value
  - `PG_RETURN_BYTEA_P` - returns bytea result
  - `ereturn` - soft error return with context support

- Called from (representative examples):
  - `[CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md)` - trigger creation with bytea parameters
  - `[string_to_datum](../s/string_to_datum.md)` - generic string to datum conversion
  - `PG_STR_GET_BYTEA` - string to bytea conversion utility
  - PostgreSQL's type input system for bytea columns

## Notes and Other Information
- The function has a known limitation: input is scanned twice, which impacts performance for large inputs
- Error checking is described as "minimal" in the source comments, focusing on basic format validation
- Supports soft error handling through the error context mechanism
- For octal format, accepts only valid 3-digit octal sequences (000-377)
- Hexadecimal format is more efficient and is the preferred modern format
- The function handles memory allocation dynamically based on input size
- Part of PostgreSQL's type system infrastructure, automatically called when converting text to bytea
- Located in `src/backend/utils/adt/varlena.c` as part of variable-length data type support
- Critical for data import/export operations involving binary data

## Simplified Source

```c
Datum byteain(PG_FUNCTION_ARGS)
{
    char *inputText = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    char *tp, *rp;
    int bc;
    bytea *result;

    // Handle hexadecimal format (\x...)
    if (inputText[0] == '\\' && inputText[1] == 'x') {
        size_t len = strlen(inputText);

        bc = (len - 2) / 2 + VARHDRSZ;  // Calculate max length
        result = palloc(bc);
        bc = hex_decode_safe(inputText + 2, len - 2, VARDATA(result), escontext);
        SET_VARSIZE(result, bc + VARHDRSZ);

        PG_RETURN_BYTEA_P(result);
    }

    // Handle traditional escaped format - First pass: calculate length
    for (bc = 0, tp = inputText; *tp != '\0'; bc++) {
        if (tp[0] != '\\') {
            tp++;
        } else if (tp[0] == '\\' && tp[1] >= '0' && tp[1] <= '3' &&
                   tp[2] >= '0' && tp[2] <= '7' && tp[3] >= '0' && tp[3] <= '7') {
            tp += 4;  // Valid octal sequence
        } else if (tp[0] == '\\' && tp[1] == '\\') {
            tp += 2;  // Escaped backslash
        } else {
            // Invalid escape sequence
            ereturn(escontext, (Datum) 0,
                   (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for type %s", "bytea")));
        }
    }

    // Allocate result
    bc += VARHDRSZ;
    result = (bytea *) palloc(bc);
    SET_VARSIZE(result, bc);

    // Second pass: convert data
    tp = inputText;
    rp = VARDATA(result);
    while (*tp != '\0') {
        if (tp[0] != '\\') {
            *rp++ = *tp++;
        } else if (tp[0] == '\\' && tp[1] >= '0' && tp[1] <= '3' &&
                   tp[2] >= '0' && tp[2] <= '7' && tp[3] >= '0' && tp[3] <= '7') {
            // Convert octal sequence to byte
            bc = VAL(tp[1]);
            bc = (bc << 3) + VAL(tp[2]);
            *rp++ = (bc << 3) + VAL(tp[3]);
            tp += 4;
        } else if (tp[0] == '\\' && tp[1] == '\\') {
            *rp++ = '\\';
            tp += 2;
        }
    }

    PG_RETURN_BYTEA_P(result);
}
```