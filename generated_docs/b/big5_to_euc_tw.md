# big5_to_euc_tw

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:65-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c#L65-L80)

## Overview
PostgreSQL function that converts text from Big5 encoding to EUC_TW (Extended Unix Code for Taiwan) encoding, serving as a wrapper for the core conversion logic.

## Definition
```c
Datum big5_to_euc_tw(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL function interface for converting character strings from Big5 encoding to EUC_TW encoding. It extracts function arguments using PostgreSQL's function call macros, validates the encoding conversion parameters, and delegates the actual conversion work to the `big52euc_tw` helper function. The conversion transforms Big5 multibyte characters into their EUC_TW equivalents, which may involve generating SS2 sequences for characters in CNS11643 planes 2-7.

The function handles error conditions based on the noError parameter and returns the number of bytes processed from the source string.

## Parameters / Member Variables
- `src`: Source string in Big5 encoding (PG_GETARG_CSTRING(2))
- `dest`: Destination buffer for EUC_TW encoded output (PG_GETARG_CSTRING(3))  
- `len`: Length of the source string in bytes (PG_GETARG_INT32(4))
- `noError`: Boolean flag indicating whether to suppress errors on invalid characters (PG_GETARG_BOOL(5))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting string arguments)
  - PG_GETARG_INT32 (macro for extracting integer arguments)
  - PG_GETARG_BOOL (macro for extracting boolean arguments)
  - CHECK_ENCODING_CONVERSION_ARGS (validates source and target encodings)
  - [big52euc_tw](big52euc_tw.md) (performs the actual character conversion)
  - PG_RETURN_INT32 (macro for returning integer result)
  - PG_BIG5 (encoding constant)
  - PG_EUC_TW (encoding constant)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL's function call mechanism)

## Notes and Other Information
- This is a PostgreSQL V1 calling convention function (uses PG_FUNCTION_ARGS)
- The function returns the number of bytes processed from the source string
- Error handling is controlled by the noError parameter - when true, conversion stops on invalid characters without throwing errors
- Part of PostgreSQL's multibyte character encoding conversion system located in src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:65-80