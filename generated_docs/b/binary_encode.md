# binary_encode

## Location
[src/backend/utils/adt/encode.c:48-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/encode.c#L48-L95)

## Overview
PostgreSQL SQL function that encodes binary data (bytea) into a text representation using a specified encoding format.

## Definition

```c
struct pg_encoding *enc;
```
## Detailed Description
The  function is a SQL-callable function that converts binary data from a  input into an encoded text format. It supports various encoding schemes like base64, hex, and escape formats. The function takes two arguments: the binary data to encode and the name of the encoding method to use.

The function performs encoding length estimation to allocate the appropriate amount of memory, then performs the actual encoding operation. It includes overflow protection to prevent memory corruption and ensures the result size doesn't exceed PostgreSQL's maximum allocation limits.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - Binary data to be encoded
  - Argument 1:  - Name of the encoding method (e.g., "base64", "hex", "escape")

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract bytea argument from function call
  -  - Extract datum argument from function call
  -  - Convert text datum to C string
  -  - Find encoding structure by name
  -  - PostgreSQL memory allocation
  -  - Set variable-length data size
  -  - Return text result from function
- Data structures used:
  -  - Encoding method structure
  -  - Maximum allocation size constant
- Called from (representative examples):
  - No direct references found (SQL-callable function)

## Notes and Other Information
- Includes overflow protection to prevent memory corruption when encoding large data
- Uses uint64 for length calculations to handle large inputs safely
- Raises FATAL error if encoding estimate is insufficient, as this indicates memory corruption
- Supports dynamic encoding method selection at runtime
- Part of PostgreSQL's binary data handling utilities in encode.c

## Simplified Source

```c
Datum binary_encode(PG_FUNCTION_ARGS) {
    // Extract input arguments
    bytea *data = PG_GETARG_BYTEA_PP(0);
    char *encoding_name = TextDatumGetCString(PG_GETARG_DATUM(1));

    // Find the encoding method
    const struct pg_encoding *enc = pg_find_encoding(encoding_name);
    if (enc == NULL)
        ereport(ERROR, (errmsg("unrecognized encoding: \"%s\"", encoding_name)));

    // Get data pointer and length
    char *dataptr = VARDATA_ANY(data);
    size_t datalen = VARSIZE_ANY_EXHDR(data);

    // Calculate output length and check for overflow
    uint64 resultlen = enc->encode_len(dataptr, datalen);
    if (resultlen > MaxAllocSize - VARHDRSZ)
        ereport(ERROR, (errmsg("result of encoding conversion is too large")));

    // Allocate result buffer and encode
    text *result = palloc(VARHDRSZ + resultlen);
    uint64 actual_len = enc->encode(dataptr, datalen, VARDATA(result));

    // Verify encoding didn't overflow (critical safety check)
    if (actual_len > resultlen)
        elog(FATAL, "overflow - encode estimate too small");

    SET_VARSIZE(result, VARHDRSZ + actual_len);
    PG_RETURN_TEXT_P(result);
}
```