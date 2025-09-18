# pg_verify_mbstr_len

## Location
[src/backend/utils/mb/mbutils.c:1597-1668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1597-L1668)

## Overview
Verifies that a multibyte string is validly encoded in the specified character encoding and returns the character count in that encoding.

## Definition
```c
int pg_verify_mbstr_len(int encoding, const char *mbstr, int len, bool noError)
```

## Detailed Description
This function validates a multibyte string and counts the number of characters (not bytes) in the specified encoding. Unlike pg_verify_mbstr, this function cannot use the faster encoding-specific mbverifystr() function because it needs to count individual characters. It processes the string character by character, using encoding-specific character verification functions.

For single-byte encodings, the function only needs to check for null bytes (\0) and returns the byte length as the character count. For multibyte encodings, it iterates through each character, using a fast path for ASCII-subset characters and the encoding-specific mbverifychar function for high-bit characters.

## Parameters / Member Variables
- `encoding`: Integer identifier for the target character encoding (must be valid according to PG_VALID_ENCODING)
- `mbstr`: Pointer to the multibyte string to be verified (not necessarily null-terminated)
- `len`: Length of the string in bytes to verify
- `noError`: Boolean flag controlling error handling behavior - if true, returns -1 on invalid encoding; if false, reports error via report_invalid_encoding

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_ENCODING (macro for encoding validation)
  - [pg_encoding_max_length](pg_encoding_max_length.md) (gets maximum bytes per character for encoding)
  - memchr (standard C library function for finding null bytes)
  - [report_invalid_encoding](../r/report_invalid_encoding.md) (error reporting function)
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - pg_wchar_table[encoding].mbverifychar (encoding-specific character verification function)
- Called from (representative examples):
  - [length_in_encoding](../l/length_in_encoding.md)

## Notes and Other Information
- Returns the number of characters in the string (not bytes) if validation succeeds
- Returns -1 only when noError is true and invalid encoding is detected
- For single-byte encodings, performs optimized null-byte checking using memchr
- Uses a fast path for ASCII characters in multibyte encodings to improve performance
- The function processes the string character by character, advancing by the byte length of each valid character
- Cannot use the faster mbverifystr function because character counting is required
- Null bytes (\0) are considered invalid in all encodings handled by this function