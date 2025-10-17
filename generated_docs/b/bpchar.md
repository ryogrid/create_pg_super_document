# bpchar

## Location
[src/backend/utils/adt/varchar.c:271-352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L271-L352)

## Overview
Converts a CHARACTER type to a specified size with appropriate truncation and padding rules for both explicit and implicit casts.

## Definition
```c
Datum bpchar(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bpchar` function is a core PostgreSQL function that handles size conversion for the CHARACTER (bpchar) data type. It implements the SQL standard's character type semantics by converting an input bpchar value to a target length specified by the typmod parameter.

The function handles two distinct scenarios based on the `isExplicit` parameter:
- For explicit casts (e.g., `CAST(value AS char(N))`): silently truncates excess characters
- For implicit casts: raises an error if non-space characters would be truncated

When the target length is larger than the input, the function pads the result with trailing spaces. The function is multi-byte character aware, using PostgreSQL's encoding-specific functions to properly handle character boundaries and lengths.

## Parameters / Member Variables
- `source` (BpChar*): Input bpchar value to be converted
- `maxlen` (int32): Target length including VARHDRSZ bytes (typmod value)
- `isExplicit` (bool): True for explicit casts, false for implicit casts

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - VARSIZE_ANY_EXHDR
  - VARDATA_ANY
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md)
  - [pg_mbcharcliplen](../p/pg_mbcharcliplen.md)
  - [palloc](../p/palloc.md)
  - SET_VARSIZE
  - VARDATA
  - memcpy
  - memset
  - PG_RETURN_BPCHAR_P
- Called from (representative examples):
  - None found in current analysis

## Notes and Other Information
- Implements SQL standard CHARACTER type conversion semantics
- Handles multi-byte character encodings correctly by using character-aware clipping functions
- The typmod parameter includes VARHDRSZ overhead, which must be subtracted for character counting
- Error handling follows PostgreSQL standards with appropriate error codes for data truncation
- Memory management uses PostgreSQL's palloc system for proper context handling
- Blank-padding with spaces is performed when the target length exceeds input length

## Simplified Source

```c
Datum bpchar(PG_FUNCTION_ARGS) {
    BpChar *source = PG_GETARG_BPCHAR_PP(0);
    int32 maxlen = PG_GETARG_INT32(1);
    bool isExplicit = PG_GETARG_BOOL(2);

    // Return source unchanged if no valid typmod
    if (maxlen < (int32) VARHDRSZ) {
        PG_RETURN_BPCHAR_P(source);
    }

    // Convert typmod to character count
    maxlen -= VARHDRSZ;

    // Extract source data
    int32 len = VARSIZE_ANY_EXHDR(source);
    char *s = VARDATA_ANY(source);
    int charlen = pg_mbstrlen_with_len(s, len);

    // Return source unchanged if already correct length
    if (charlen == maxlen) {
        PG_RETURN_BPCHAR_P(source);
    }

    BpChar *result;
    char *r;

    if (charlen > maxlen) {
        // Truncate excess characters
        size_t maxmblen = pg_mbcharcliplen(s, len, maxlen);

        // For implicit casts, verify truncated chars are spaces
        if (!isExplicit) {
            for (int i = maxmblen; i < len; i++) {
                if (s[i] != ' ') {
                    ereport(ERROR, "value too long for type character");
                }
            }
        }

        len = maxmblen;
        maxlen = len;  // Now represents byte length
    } else {
        // Calculate byte length needed for padding
        maxlen = len + (maxlen - charlen);
    }

    // Allocate and populate result
    result = palloc(maxlen + VARHDRSZ);
    SET_VARSIZE(result, maxlen + VARHDRSZ);
    r = VARDATA(result);

    // Copy data and pad with spaces if needed
    memcpy(r, s, len);
    if (maxlen > len) {
        memset(r + len, ' ', maxlen - len);
    }

    PG_RETURN_BPCHAR_P(result);
}
```