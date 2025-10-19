# euc_tw_to_big5

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:49-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c#L49-L64)

## Overview
PostgreSQL function that converts text from EUC_TW (Extended Unix Code for Taiwan) encoding to Big5 encoding, serving as a wrapper for the core conversion logic.

## Definition

```c
Datum
euc_tw_to_big5(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL function interface for converting character strings from EUC_TW encoding to Big5 encoding. It extracts the function arguments using PostgreSQL's function call macros, validates the encoding conversion parameters, and delegates the actual conversion work to the  helper function. The function handles both successful conversions and error conditions based on the  parameter.

The conversion process involves translating EUC_TW multibyte characters (which can include characters from multiple CNS11643 planes) into their Big5 equivalents. EUC_TW uses SS2 (Single Shift 2) sequences to access additional character planes beyond the basic plane.

## Parameters / Member Variables
- : Source string in EUC_TW encoding (PG_GETARG_CSTRING(2))
- : Destination buffer for Big5 encoded output (PG_GETARG_CSTRING(3))
- : Length of the source string in bytes (PG_GETARG_INT32(4))
- : Boolean flag indicating whether to suppress errors on invalid characters (PG_GETARG_BOOL(5))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting string arguments)
  - PG_GETARG_INT32 (macro for extracting integer arguments)
  - PG_GETARG_BOOL (macro for extracting boolean arguments)
  - CHECK_ENCODING_CONVERSION_ARGS (validates source and target encodings)
  - [euc_tw2big5](euc_tw2big5.md) (performs the actual character conversion)
  - PG_RETURN_INT32 (macro for returning integer result)
  - PG_EUC_TW (encoding constant)
  - PG_BIG5 (encoding constant)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL's function call mechanism)

## Notes and Other Information
- This is a PostgreSQL V1 calling convention function (uses PG_FUNCTION_ARGS)
- The function returns the number of bytes processed from the source string
- Error handling is controlled by the noError parameter - [when](../w/when.md) true, conversion stops on invalid characters without throwing errors
- Part of PostgreSQL's multibyte character encoding conversion system located in src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:49-64

## Simplified Source

```c
Datum
euc_tw_to_big5(PG_FUNCTION_ARGS)
{
    // Extract function parameters
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
    int len = PG_GETARG_INT32(4);
    bool noError = PG_GETARG_BOOL(5);

    // Validate encoding conversion request
    CHECK_ENCODING_CONVERSION_ARGS(PG_EUC_TW, PG_BIG5);

    // Perform the actual conversion and return result
    int converted = euc_tw2big5(src, dest, len, noError);
    PG_RETURN_INT32(converted);
}
```