# win1250_to_latin2

## Location
[src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c:166-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c#L166-L180)

## Overview
A PostgreSQL encoding conversion function that converts text from Windows-1250 encoding to Latin-2 (ISO-8859-2) encoding for Central European languages.

## Definition

```c
Datum
win1250_to_latin2(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL conversion procedure that transforms character strings from Windows-1250 encoding to Latin-2 (ISO-8859-2) encoding. Both encodings support Central and Eastern European languages, but Windows-1250 is Microsoft's proprietary encoding while Latin-2 is an ISO standard.

The function operates by using a direct character-to-character mapping table () that translates each Windows-1250 character code point (128-255) to its corresponding Latin-2 equivalent. Characters in the ASCII range (0-127) are identical in both encodings and are copied directly.

The conversion process handles unmappable characters gracefully - when a Windows-1250 character has no equivalent in Latin-2, the function can either report an error or stop conversion depending on the  parameter setting.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 2 (): Source string in Windows-1250 encoding (null-terminated C string)
  - Argument 3 (): Destination buffer for Latin-2 encoded output (null-terminated C string)
  - Argument 4 (): Length of the source string in bytes
  - Argument 5 (): Boolean flag - if true, conversion stops on unmappable characters without throwing an error; if false, throws an error on conversion failures

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract C string arguments from PostgreSQL function parameters
  -  - Extract 32-bit integer argument 
  -  - Extract boolean argument
  -  - Validate source and destination encoding parameters
  -  - Generic single-byte character set conversion function
  -  - Return 32-bit integer result
  -  - Character mapping table from Windows-1250 to ISO-8859-2
  -  - PostgreSQL encoding constant for Windows-1250
  -  - PostgreSQL encoding constant for Latin-2/ISO-8859-2
- Called from (representative examples):
  - No direct references found in the codebase - likely called through PostgreSQL's encoding conversion system

## Notes and Other Information
- The function is registered as a PostgreSQL conversion procedure through 
- Returns the number of input bytes successfully converted as a 32-bit integer
- Both Windows-1250 and Latin-2 are single-byte character encodings primarily used for Central and Eastern European languages including Polish, Czech, Slovak, Hungarian, Slovenian, and Croatian
- The conversion uses a static lookup table that maps the extended character range (128-255) between the two encodings
- Some Windows-1250 characters have no Latin-2 equivalent (represented as 0x00 in the mapping table), which will trigger conversion errors unless noError is set to true
- File location: src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c:166-180

## Simplified Source

```c
Datum win1250_to_latin2(PG_FUNCTION_ARGS) {
    // Extract function arguments
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);   // Source Windows-1250 string
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);  // Destination buffer
    int len = PG_GETARG_INT32(4);                                  // Source length
    bool noError = PG_GETARG_BOOL(5);                              // Error handling flag

    // Validate encoding conversion arguments
    CHECK_ENCODING_CONVERSION_ARGS(PG_WIN1250, PG_LATIN2);

    // Perform direct Windows-1250 to Latin-2 conversion using translation table
    int converted = local2local(src, dest, len, PG_WIN1250, PG_LATIN2,
                               win1250_2_iso88592, noError);

    // Return number of bytes converted
    PG_RETURN_INT32(converted);
}
``` 