# conninfo_uri_decode

## Location
[src/interfaces/libpq/fe-connect.c:6749-6816](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L6749-L6816)

## Overview
Decodes percent-encoded URI strings by converting %xy hexadecimal sequences to their corresponding characters while preserving non-encoded characters.

## Definition

```c
static char *
conninfo_uri_decode(const char *str, PQExpBuffer errorMessage)
```
## Detailed Description
This function implements URI percent-decoding according to RFC 3986 standards. It processes a string containing percent-encoded tokens and returns a newly allocated decoded string. The decoding process:

1. Scans the input string character by character
2. Copies non-encoded characters directly to the output
3. For percent-encoded sequences (%xy):
   - Validates that exactly two hexadecimal digits follow the '%'
   - Converts the hex digits to their numeric value
   - Combines the digits to form the decoded byte value
   - Forbids null bytes (%00) for security reasons
4. Allocates memory for the decoded result and handles allocation failures

The function provides comprehensive error handling for malformed percent-encoded sequences and memory allocation failures.

## Parameters / Member Variables
- : The percent-encoded input string to decode
- : Buffer to store error messages if decoding fails

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - [get_hexdigit](../g/get_hexdigit.md)
  - [libpq_append_error](../l/libpq_append_error.md)
  - strlen
  - free
- Called from (representative examples):
  - [conninfo_uri_parse_params](conninfo_uri_parse_params.md)
  - [conninfo_storeval](conninfo_storeval.md)
  - internalPQconninfoOption

## Notes and Other Information
- Returns malloc'd decoded string on success, NULL on failure
- Caller is responsible for freeing the returned string
- Forbids %00 sequences to prevent null byte injection attacks
- Case-insensitive hexadecimal digit handling (A-F and a-f both accepted)
- Validates that percent signs are followed by exactly two hex digits
- Memory allocation uses strlen() + 1 since decoded string is never longer than original
- Provides detailed error messages for debugging malformed URI components