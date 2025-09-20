# euc_kr_to_mic

## Location
[src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c:41-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c#L41-L56)

## Overview
Converts text from EUC-KR encoding to PostgreSQL's internal MULE encoding format, serving as the PostgreSQL function interface for EUC-KR to MULE conversion.

## Definition

```c
Datum
euc_kr_to_mic(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL-callable wrapper for EUC-KR to MULE (Multi-byte Universal Language Environment) encoding conversion. It extracts the necessary parameters from PostgreSQL's function argument structure, validates the encoding conversion request, and delegates the actual conversion work to the euc_kr2mic function. The function is designed to be called from PostgreSQL's encoding conversion system when converting text data from EUC-KR (Extended Unix Code for Korean) to PostgreSQL's internal MULE representation.

## Parameters / Member Variables
- : PostgreSQL's standard function argument structure containing:
  -  (argument 2): Source string in EUC-KR encoding to be converted
  -  (argument 3): Destination buffer for the converted MULE-encoded string
  -  (argument 4): Length of the source string in bytes
  -  (argument 5): Boolean flag indicating whether to suppress conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (extracts string arguments)
  - PG_GETARG_INT32 (extracts integer argument)
  - PG_GETARG_BOOL (extracts boolean argument)
  - CHECK_ENCODING_CONVERSION_ARGS (validates conversion request)
  - [euc_kr2mic](euc_kr2mic.md) (performs the actual encoding conversion)
  - PG_RETURN_INT32 (returns the result)
- Constants referenced:
  - PG_EUC_KR (EUC-KR encoding identifier)
  - PG_MULE_INTERNAL (MULE internal encoding identifier)
- Called from:
  - PostgreSQL encoding conversion system (no direct references found)

## Notes and Other Information
- This is a PostgreSQL-specific wrapper function that follows the standard PostgreSQL function interface pattern
- The function validates that the conversion is from EUC-KR to MULE internal format before proceeding
- Returns the number of bytes converted as an integer value
- Part of PostgreSQL's multi-byte character encoding conversion subsystem