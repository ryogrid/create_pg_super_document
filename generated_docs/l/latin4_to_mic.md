# latin4_to_mic

## Location
[src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c:107-122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c#L107-L122)

## Overview
A PostgreSQL encoding conversion function that converts text from Latin-4 (ISO 8859-4) encoding to the Multi-byte Internal Code (MIC) encoding used internally by PostgreSQL's multi-byte character system.

## Definition

```c
Datum
latin4_to_mic(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character data from Latin-4 encoding to MIC (Multi-byte Internal Code) encoding. It acts as a PostgreSQL function interface wrapper around the lower-level  conversion routine. The function follows PostgreSQL's standard conversion function protocol and specifically handles the conversion from PG_LATIN4 to PG_MULE_INTERNAL encoding types. Latin-4 (ISO 8859-4) is designed for North European languages including Estonian, Latvian, Lithuanian, Greenlandic, and Sami languages.

## Parameters / Member Variables
-  (src): Source string buffer containing Latin-4 encoded text to be converted
-  (dest): Destination string buffer where MIC encoded result will be stored
-  (len): Length of the source string in bytes
-  (noError): Boolean flag indicating whether to suppress errors on conversion failure

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - [latin2mic](latin2mic.md)
  - PG_RETURN_INT32
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- The function validates the encoding conversion arguments using CHECK_ENCODING_CONVERSION_ARGS macro
- Uses LC_ISO8859_4 locale constant when calling the underlying latin2mic function
- Returns the number of bytes successfully converted as an integer
- Part of PostgreSQL's character set conversion infrastructure
- Located in the latin_and_mic conversion module
- Follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- Latin-4 encoding supports characters for North European languages including Baltic languages (Estonian, Latvian, Lithuanian), Greenlandic, and Sami languages

## Simplified Source

```c
Datum latin4_to_mic(PG_FUNCTION_ARGS) {
    // Extract function arguments
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);   // Source Latin-4 string
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);  // Destination buffer
    int len = PG_GETARG_INT32(4);                                  // Source length
    bool noError = PG_GETARG_BOOL(5);                              // Error handling flag

    // Validate encoding conversion arguments
    CHECK_ENCODING_CONVERSION_ARGS(PG_LATIN4, PG_MULE_INTERNAL);

    // Convert Latin-4 to MIC using latin2mic with ISO 8859-4 character set
    int converted = latin2mic(src, dest, len, LC_ISO8859_4, PG_LATIN4, noError);

    // Return number of bytes converted
    PG_RETURN_INT32(converted);
}
```