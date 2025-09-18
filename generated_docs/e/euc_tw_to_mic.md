# euc_tw_to_mic

## Location
src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c: 81 - 96

## Overview
PostgreSQL function that converts text from EUC_TW (Extended Unix Code for Taiwan) encoding to MIC (Mule Internal Code) encoding, serving as a wrapper for the core conversion logic.

## Definition
```c
Datum euc_tw_to_mic(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL function interface for converting character strings from EUC_TW encoding to MIC (Mule Internal Code) encoding. MIC is PostgreSQL's internal multibyte character representation that can handle multiple character sets simultaneously. The function extracts function arguments using PostgreSQL's macros, validates the encoding parameters, and delegates the actual conversion work to the `euc_tw2mic` helper function.

The conversion process transforms EUC_TW characters (including those from different CNS11643 planes accessed via SS2 sequences) into MIC format, which uses leading bytes to identify different character sets.

## Parameters / Member Variables
- `src`: Source string in EUC_TW encoding (PG_GETARG_CSTRING(2))
- `dest`: Destination buffer for MIC encoded output (PG_GETARG_CSTRING(3))
- `len`: Length of the source string in bytes (PG_GETARG_INT32(4))
- `noError`: Boolean flag indicating whether to suppress errors on invalid characters (PG_GETARG_BOOL(5))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (macro for extracting string arguments)
  - PG_GETARG_INT32 (macro for extracting integer arguments)  
  - PG_GETARG_BOOL (macro for extracting boolean arguments)
  - CHECK_ENCODING_CONVERSION_ARGS (validates source and target encodings)
  - [euc_tw2mic](euc_tw2mic.md) (performs the actual character conversion)
  - PG_RETURN_INT32 (macro for returning integer result)
  - PG_EUC_TW (encoding constant)
  - PG_MULE_INTERNAL (encoding constant)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL's function call mechanism)

## Notes and Other Information
- This is a PostgreSQL V1 calling convention function (uses PG_FUNCTION_ARGS)
- The function returns the number of bytes processed from the source string
- MIC encoding is PostgreSQL's internal format that can represent multiple character sets in a single string
- Error handling is controlled by the noError parameter - when true, conversion stops on invalid characters without throwing errors
- Part of PostgreSQL's multibyte character encoding conversion system located in src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c:81-96