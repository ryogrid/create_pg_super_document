# mic_to_euc_kr

## Location
[src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c:57-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c#L57-L75)

## Overview
Converts text from PostgreSQL's internal MULE encoding to EUC-KR encoding, serving as the PostgreSQL function interface for MULE to EUC-KR conversion.

## Definition
```c
Datum mic_to_euc_kr(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL-callable wrapper for MULE to EUC-KR encoding conversion. It extracts the necessary parameters from PostgreSQL's function argument structure, validates the encoding conversion request, and delegates the actual conversion work to the mic2euc_kr function. The function is designed to be called from PostgreSQL's encoding conversion system when converting text data from PostgreSQL's internal MULE (Multi-byte Universal Language Environment) representation to EUC-KR (Extended Unix Code for Korean) encoding.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL's standard function argument structure containing:
  - `src` (argument 2): Source string in MULE encoding to be converted
  - `dest` (argument 3): Destination buffer for the converted EUC-KR-encoded string
  - `len` (argument 4): Length of the source string in bytes
  - `noError` (argument 5): Boolean flag indicating whether to suppress conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (extracts string arguments)
  - PG_GETARG_INT32 (extracts integer argument)
  - PG_GETARG_BOOL (extracts boolean argument)
  - CHECK_ENCODING_CONVERSION_ARGS (validates conversion request)
  - [mic2euc_kr](mic2euc_kr.md) (performs the actual encoding conversion)
  - PG_RETURN_INT32 (returns the result)
- Constants referenced:
  - PG_MULE_INTERNAL (MULE internal encoding identifier)
  - PG_EUC_KR (EUC-KR encoding identifier)
- Called from:
  - PostgreSQL encoding conversion system (no direct references found)

## Notes and Other Information
- This is a PostgreSQL-specific wrapper function that follows the standard PostgreSQL function interface pattern
- The function validates that the conversion is from MULE internal format to EUC-KR before proceeding
- Returns the number of bytes converted as an integer value
- Part of PostgreSQL's multi-byte character encoding conversion subsystem
- Complementary function to euc_kr_to_mic, providing bidirectional conversion capability