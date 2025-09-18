# mic_to_latin4

## Location
src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c: 123 - 136

## Overview
This function converts text encoded in MULE Internal Code (MIC) to Latin-4 (ISO 8859-4) character encoding, providing a PostgreSQL-compatible conversion interface.

## Definition
```c
Datum mic_to_latin4(PG_FUNCTION_ARGS)
```

## Detailed Description
`mic_to_latin4` is a PostgreSQL conversion function that converts text from MULE Internal Code (MIC) encoding to Latin-4 (ISO 8859-4) encoding. This function is part of PostgreSQL's multibyte character encoding conversion system and follows the standard PostgreSQL function calling convention using `PG_FUNCTION_ARGS`.

The function serves as a wrapper around the lower-level `mic2latin` conversion routine, specifically configured for Latin-4 character set conversion. It validates the encoding conversion arguments and performs the actual conversion with proper error handling.

## Parameters / Member Variables
- `src` (PG_GETARG_CSTRING(2)): Source string in MULE Internal Code encoding to be converted
- `dest` (PG_GETARG_CSTRING(3)): Destination buffer where the converted Latin-4 encoded string will be stored
- `len` (PG_GETARG_INT32(4)): Length of the source string in bytes
- `noError` (PG_GETARG_BOOL(5)): Boolean flag indicating whether to suppress conversion errors (true) or raise them (false)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CSTRING` (PostgreSQL argument extraction macro)
  - `PG_GETARG_INT32` (PostgreSQL argument extraction macro)
  - `PG_GETARG_BOOL` (PostgreSQL argument extraction macro)
  - `CHECK_ENCODING_CONVERSION_ARGS` (encoding validation macro)
  - [mic2latin](mic2latin.md) (core conversion function)
  - `PG_RETURN_INT32` (PostgreSQL return value macro)
  - `PG_MULE_INTERNAL` (encoding constant)
  - `PG_LATIN4` (encoding constant)
  - `LC_ISO8859_4` (locale constant)
- Called from: 
  - This function is typically registered as a conversion procedure in PostgreSQL's encoding conversion system

## Notes and Other Information
- This function is located in `src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c:123-136`
- Latin-4 (ISO 8859-4) is designed to cover languages like Estonian, Latvian, Lithuanian, Greenlandic, and Sami
- The function returns the number of bytes converted as an integer
- MULE Internal Code is an internal encoding format used by PostgreSQL for handling multibyte character sets
- Error handling behavior depends on the `noError` parameter - when true, invalid characters may be silently skipped or replaced
- The conversion uses the `LC_ISO8859_4` locale specification to ensure proper character mapping