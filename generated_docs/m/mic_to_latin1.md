# mic_to_latin1

## Location
[src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c:59-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c#L59-L74)

## Overview
A PostgreSQL encoding conversion function that converts text from the Multi-byte Internal Code (MIC) encoding to Latin-1 (ISO 8859-1) encoding, performing the reverse conversion of latin1_to_mic.

## Definition

```c
Datum
mic_to_latin1(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms character data from MIC (Multi-byte Internal Code) encoding back to Latin-1 encoding. It acts as a PostgreSQL function interface wrapper around the lower-level  conversion routine. The function follows PostgreSQL's standard conversion function protocol and specifically handles the conversion from PG_MULE_INTERNAL to PG_LATIN1 encoding types. This is the inverse operation of the latin1_to_mic function.

## Parameters / Member Variables
-  (src): Source string buffer containing MIC encoded text to be converted
-  (dest): Destination string buffer where Latin-1 encoded result will be stored
-  (len): Length of the source string in bytes
-  (noError): Boolean flag indicating whether to suppress errors on conversion failure

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - CHECK_ENCODING_CONVERSION_ARGS
  - [mic2latin](mic2latin.md)
  - PG_RETURN_INT32
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- The function validates the encoding conversion arguments using CHECK_ENCODING_CONVERSION_ARGS macro
- Uses LC_ISO8859_1 locale constant when calling the underlying mic2latin function
- Returns the number of bytes successfully converted as an integer
- Part of PostgreSQL's character set conversion infrastructure
- Located in the latin_and_mic conversion module
- Follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- Performs the reverse operation of latin1_to_mic function

## Simplified Source

```c
Datum mic_to_latin1(PG_FUNCTION_ARGS) {
    // Extract function arguments
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);   // Source MIC string
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);  // Destination buffer
    int len = PG_GETARG_INT32(4);                                  // Source length
    bool noError = PG_GETARG_BOOL(5);                              // Error handling flag

    // Validate encoding conversion arguments
    CHECK_ENCODING_CONVERSION_ARGS(PG_MULE_INTERNAL, PG_LATIN1);

    // Convert MIC to Latin-1 using mic2latin with ISO 8859-1 character set
    int converted = mic2latin(src, dest, len, LC_ISO8859_1, PG_LATIN1, noError);

    // Return number of bytes converted
    PG_RETURN_INT32(converted);
}
```