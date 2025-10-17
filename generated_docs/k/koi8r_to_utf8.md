# koi8r_to_utf8

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_cyrillic/utf8_and_cyrillic.c:66-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_cyrillic/utf8_and_cyrillic.c#L66-L86)

## Overview
Converts KOI8-R (Russian Cyrillic) encoded text to UTF-8 encoding within PostgreSQL's character encoding conversion system.

## Definition

```c
Datum
koi8r_to_utf8(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms KOI8-R encoded strings into UTF-8 encoding. KOI8-R is a character encoding designed for the Russian language using Cyrillic script. The function implements PostgreSQL's standard conversion procedure interface, accepting source and destination buffers along with conversion parameters. It utilizes the LocalToUtf utility function with a KOI8-R-specific Unicode conversion tree (koi8r_to_unicode_tree) to perform the actual character mapping from KOI8-R byte sequences to UTF-8 Unicode codepoints.

## Parameters / Member Variables
- : Source KOI8-R encoded string (null-terminated C string)
- : Destination buffer for UTF-8 encoded output (null-terminated C string)
- : Length of the source string in bytes
- : Error handling flag - if true, don't throw an error if conversion fails

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (parameter extraction)
  - PG_GETARG_INT32 (parameter extraction)
  - PG_GETARG_BOOL (parameter extraction)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [LocalToUtf](../L/LocalToUtf.md) (core local encoding to UTF-8 conversion function)
  - PG_RETURN_INT32 (return value macro)
  - koi8r_to_unicode_tree (KOI8-R to Unicode conversion table)
- Called from:
  - No direct references found (likely registered as encoding conversion procedure)

## Notes and Other Information
- This function follows PostgreSQL's standard conversion procedure interface, returning the number of bytes successfully converted
- Uses encoding constants PG_KOI8R and PG_UTF8 for validation
- Part of the utf8_and_cyrillic conversion module
- KOI8-R encoding is primarily used for Russian text and was widely used in Soviet-era computing systems
- The conversion relies on a Unicode tree structure for efficient character mapping from KOI8-R to Unicode codepoints
- Error handling is controlled by the noError parameter, allowing for graceful failure handling when requested
- Complements utf8_to_koi8r for bidirectional conversion between UTF-8 and KOI8-R encodings

## Simplified Source

```c
Datum
koi8r_to_utf8(PG_FUNCTION_ARGS)
{
    // Extract function parameters
    unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
    unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
    int len = PG_GETARG_INT32(4);
    bool noError = PG_GETARG_BOOL(5);

    // Validate encoding conversion arguments
    CHECK_ENCODING_CONVERSION_ARGS(PG_KOI8R, PG_UTF8);

    // Convert KOI8-R to UTF-8 using conversion tree
    int converted = LocalToUtf(src, len, dest,
                              &koi8r_to_unicode_tree,
                              NULL, 0, NULL,
                              PG_KOI8R, noError);

    return converted;
}
```