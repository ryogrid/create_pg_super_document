# text_length

## Location
[src/backend/utils/adt/varlena.c:711-730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L711-L730)

## Overview
Internal function that efficiently calculates the character length of a text datum, with optimizations for different database encodings and compression states.

## Definition
```c
static int32 text_length(Datum str)
```

## Detailed Description
The `text_length` function performs the core work of calculating the logical character length of a PostgreSQL text value. It implements important optimizations based on the database encoding: for single-byte encodings, it can calculate length without decompressing the data, while for multi-byte encodings, it must decompress and use character-aware length calculation.

The function is designed as an internal utility that can be called by various string processing functions throughout PostgreSQL. It takes a Datum parameter to indicate that the text may still be in compressed (TOAST) form, allowing for efficient processing when decompression can be avoided.

## Parameters / Member Variables
- `str`: A Datum containing a text value that may be compressed
- Return: int32 representing the number of characters in the text

## Dependencies
- Functions called/Symbols referenced:
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md) (function to get max encoding byte length)
  - [toast_raw_datum_size](toast_raw_datum_size.md) (function to get size of TOAST datum without decompressing)
  - VARHDRSZ (macro for variable-length header size)
  - DatumGetTextPP (macro to extract text from datum, potentially decompressing)
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md) (function to calculate character length for multi-byte strings)
  - VARDATA_ANY (macro to get data portion of variable-length value)
  - VARSIZE_ANY_EXHDR (macro to get size excluding header)

- Called from (representative examples):
  - [textlen](textlen.md) (wrapper function at src/backend/utils/adt/varlena.c:698)
  - [textoverlay_no_len](textoverlay_no_len.md) (at src/backend/utils/adt/varlena.c:1111)
  - DatumGetVarStringPP (at src/backend/utils/adt/varlena.c:127)
  - [escape_string](../e/escape_string.md) (multiple calls in src/bin/psql/tab-complete.c)

## Notes and Other Information
- The function is located in src/backend/utils/adt/varlena.c at lines 711-730
- Static function, intended for internal use within the module
- Implements a critical optimization: single-byte encodings can use raw size calculation
- For multi-byte encodings, must perform character-aware length calculation
- Avoids unnecessary decompression when possible for performance
- Used extensively throughout PostgreSQL for string length operations
- The fastpath optimization significantly improves performance for ASCII/Latin1 databases