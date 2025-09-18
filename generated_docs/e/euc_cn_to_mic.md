# euc_cn_to_mic

## Location
src/backend/utils/mb/conversion_procs/euc_cn_and_mic/euc_cn_and_mic.c: 41 - 56

## Overview
PostgreSQL function that serves as the entry point for converting text from EUC-CN (Extended Unix Code for Chinese) encoding to MIC (Multi-byte Internal Code) encoding.

## Definition
```c
Datum euc_cn_to_mic(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL conversion function that handles the conversion of text data from EUC-CN encoding to PostgreSQL's internal Multi-byte Internal Code (MIC) format. It acts as a wrapper around the core conversion logic, following PostgreSQL's function call convention for encoding conversion functions. The function extracts parameters from the PostgreSQL function call arguments, validates the encoding conversion request, and delegates the actual conversion work to the euc_cn2mic helper function.

## Parameters / Member Variables
The function uses PostgreSQL's PG_FUNCTION_ARGS macro and extracts:
- Source buffer (argument 2): Pointer to the input text in EUC-CN encoding
- Destination buffer (argument 3): Pointer to the output buffer for MIC encoded text  
- Length (argument 4): Length of the input text in bytes
- noError flag (argument 5): Boolean flag indicating whether to suppress conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (PostgreSQL argument extraction macro)
  - PG_GETARG_INT32 (PostgreSQL argument extraction macro)
  - PG_GETARG_BOOL (PostgreSQL argument extraction macro)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [euc_cn2mic](euc_cn2mic.md) (core conversion function)
  - PG_RETURN_INT32 (PostgreSQL return macro)
- Called from:
  - PostgreSQL encoding conversion system (no direct references found)

## Notes and Other Information
- This is a PostgreSQL-callable function that follows the standard PostgreSQL function interface
- Uses the PG_EUC_CN and PG_MULE_INTERNAL encoding constants for validation
- Returns the number of bytes converted as an integer
- The actual conversion logic is implemented in the euc_cn2mic helper function
- Part of the EUC-CN encoding conversion module in PostgreSQL's multibyte support