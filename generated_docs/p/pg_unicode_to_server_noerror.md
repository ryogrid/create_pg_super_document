# pg_unicode_to_server_noerror

## Location
src/backend/utils/mb/mbutils.c: 926 - 978

## Overview
A non-throwing variant of pg_unicode_to_server() that converts Unicode code points to server encoding and returns success/failure status instead of raising errors.

## Definition
```c
bool pg_unicode_to_server_noerror(pg_wchar c, unsigned char *s)
```

## Detailed Description
This function provides the same Unicode-to-server encoding conversion as `pg_unicode_to_server()` but with graceful error handling. Instead of throwing errors on conversion failures, it returns a boolean success indicator:

1. **Validation without errors**: Returns false for invalid Unicode code points instead of throwing errors
2. **ASCII optimization**: Handles ASCII range (≤ 0x7F) directly with guaranteed success
3. **UTF-8 server encoding**: Direct conversion when database uses UTF-8 encoding
4. **Graceful conversion failure**: Returns false when conversion functions are unavailable or conversion fails
5. **Conversion success verification**: Checks that the entire input was consumed during conversion to determine success

The function maintains the same optimization paths as its error-throwing counterpart but provides safer operation in contexts where error handling is preferred over exceptions.

## Parameters / Member Variables
- `c`: Unicode code point (pg_wchar) to convert
- `s`: Output buffer with at least MAX_UNICODE_EQUIVALENT_STRING+1 bytes available, receives null-terminated converted string on success

## Dependencies
- Functions called/Symbols referenced:
  - is_valid_unicode_codepoint (Unicode validation)
  - GetDatabaseEncoding (database encoding detection)
  - unicode_to_utf8 (Unicode to UTF-8 conversion)
  - pg_utf_mblen (UTF-8 character length calculation)
  - FunctionCall6/DatumGetInt32 (PostgreSQL function call interface)
- Called from (representative examples):
  - No direct references found in current codebase (utility function)

## Notes and Other Information
- This is the error-safe variant of pg_unicode_to_server()
- Returns true on successful conversion, false on any failure (invalid input, missing conversion function, conversion error)
- Uses the same cached conversion procedures (Utf8ToServerConvProc) as the error-throwing version
- Passes `true` as the noError parameter to the underlying conversion function
- Verifies conversion success by ensuring all input bytes were consumed
- Safe for use in contexts where exception handling is not desired or available
- Located in src/backend/utils/mb/mbutils.c:926-978