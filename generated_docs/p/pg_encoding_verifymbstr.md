# pg_encoding_verifymbstr

## Location
[src/common/wchar.c:2202-2212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L2202-L2212)

## Overview
Verifies that an entire string is valid for the specified encoding and returns the number of valid input bytes.

## Definition
```c
int pg_encoding_verifymbstr(int encoding, const char *mbstr, int len)
```

## Detailed Description
This function validates an entire string according to the specified encoding rules, verifying that all characters in the string are properly formed. Unlike `pg_encoding_verifymbchar` which only validates a single character, this function processes the entire string and returns the byte offset of the first invalid character, or the full string length if the entire string is valid.

The function is essential for data integrity validation before storing or processing multibyte text. It ensures that incoming data conforms to the expected encoding and helps prevent malformed character sequences from corrupting the database or causing processing errors.

The verification follows these rules:
- Returns the number of valid bytes from the start of the string (≤ len)
- If the entire string is valid, returns the full length (`len`)
- If invalid characters are found, returns the byte offset of the first invalid character
- Must test for and reject embedded zero bytes in the input
- Processes the complete string, not just the first character

## Parameters / Member Variables
- `encoding`: The character encoding identifier to use for validation
- `mbstr`: Pointer to the string to verify
- `len`: The length of the string to verify

## Dependencies
- Functions called/Symbols referenced:
  - `PG_VALID_ENCODING`: Macro to validate encoding identifier
  - `PG_SQL_ASCII`: Fallback encoding constant used for invalid encodings
  - `pg_wchar_table[].mbverifystr`: Encoding-specific string verification function

- Called from (representative examples):
  - [CopyConvertBuf](../C/CopyConvertBuf.md): COPY command data validation (copyfromparse.c)
  - `add_file_to_manifest`: Backup manifest generation (write_manifest.c)
  - [PQescapeInternal](../P/PQescapeInternal.md): libpq string escaping functions (fe-exec.c)
  - [test_one_vector_escape](../t/test_one_vector_escape.md): Test functions for string escaping validation
  - [test_enc_setup](../t/test_enc_setup.md), `test_enc_conversion`: Regression test encoding validation

## Notes and Other Information
- This function is defined in src/common/wchar.c:2202-2212
- Companion function to `pg_encoding_verifymbchar` for whole-string validation
- Critical for preventing malformed multibyte sequences from entering the system
- Used extensively in data input validation, particularly in COPY operations and client interfaces
- The return value allows callers to determine exactly where in the string validation failed
- Essential component of PostgreSQL's character encoding validation infrastructure
- Helps ensure data consistency across different character encodings and prevents encoding-related corruption
- Used in both server-side and client-side code for comprehensive validation coverage