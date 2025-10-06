# ascii

## Location
[src/backend/utils/adt/oracle_compat.c:925-1005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L925-L1005)

## Overview
Returns the decimal ASCII value (or Unicode code point) of the first character in a text string.

## Definition
```c
Datum ascii(PG_FUNCTION_ARGS)
```

## Detailed Description
The ascii function extracts the numeric value of the first character from a text string. The behavior varies based on the database encoding: for UTF-8 databases, it returns the Unicode code point of the first character; for other multibyte encodings, it returns the ASCII value (1-127) or raises an error for non-ASCII characters; for single-byte encodings, it returns the byte value (1-255). If the input string is empty, the function returns 0. The function includes special UTF-8 decoding logic to properly handle multibyte Unicode sequences and convert them to their corresponding code points.

## Parameters / Member Variables
- `string`: The input text string from which to extract the first character's numeric value

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - PG_UTF8
  - [pg_encoding_max_length](../p/pg_encoding_max_length.md)
- Called from (representative examples):
  - [pg_to_ascii](../p/pg_to_ascii.md) (src/backend/utils/adt/ascii.c)
  - RANGE_160 (src/backend/utils/adt/ascii.c)
  - [float4out](../f/float4out.md) (src/backend/utils/adt/float.c)
  - [float8out_internal](../f/float8out_internal.md) (src/backend/utils/adt/float.c)

## Notes and Other Information
- Located in src/backend/utils/adt/oracle_compat.c:925-1005
- Part of PostgreSQL's Oracle compatibility layer
- For UTF-8 encoding, implements manual Unicode decoding for characters > 127
- Uses bit manipulation to decode UTF-8 multibyte sequences (2, 3, or 4 bytes)
- For multibyte encodings other than UTF-8, restricts input to ASCII range (1-127)
- Returns 0 for empty strings as a special case
- The UTF-8 decoding logic handles the standard UTF-8 encoding format with proper validation
- Includes error handling for characters that are too large for the current encoding

## Simplified Source

```c
Datum
ascii(PG_FUNCTION_ARGS)
{
    text *string = PG_GETARG_TEXT_PP(0);
    int encoding = GetDatabaseEncoding();
    unsigned char *data;

    // Return 0 for empty strings
    if (VARSIZE_ANY_EXHDR(string) <= 0)
        PG_RETURN_INT32(0);

    data = (unsigned char *) VARDATA_ANY(string);

    // Handle UTF-8 encoding - decode Unicode code points
    if (encoding == PG_UTF8 && *data > 127) {
        int result = 0, tbytes = 0;

        // Determine UTF-8 sequence length and extract initial bits
        if (*data >= 0xF0) {
            result = *data & 0x07;
            tbytes = 3;
        } else if (*data >= 0xE0) {
            result = *data & 0x0F;
            tbytes = 2;
        } else {
            result = *data & 0x1f;
            tbytes = 1;
        }

        // Process continuation bytes
        for (int i = 1; i <= tbytes; i++) {
            result = (result << 6) + (data[i] & 0x3f);
        }

        PG_RETURN_INT32(result);
    } else {
        // For other encodings, check ASCII range for multibyte encodings
        if (pg_encoding_max_length(encoding) > 1 && *data > 127)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmsg("requested character too large")));

        PG_RETURN_INT32((int32) *data);
    }
}
```