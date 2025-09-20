# utf8_to_koi8u

## Location
[src/backend/utils/mb/conversion_procs/utf8_and_cyrillic/utf8_and_cyrillic.c:87-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/utf8_and_cyrillic/utf8_and_cyrillic.c#L87-L107)

## Overview
Converts UTF-8 encoded text to KOI8-U (Ukrainian Cyrillic) encoding within PostgreSQL's character encoding conversion system.

## Definition

```c
Datum
utf8_to_koi8u(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL conversion procedure that transforms UTF-8 encoded strings into KOI8-U encoding. KOI8-U is a character encoding designed for the Ukrainian language using Cyrillic script, extending KOI8-R to include additional Ukrainian-specific characters. The function implements PostgreSQL's standard conversion procedure interface, accepting source and destination buffers along with conversion parameters. It utilizes the UtfToLocal utility function with a KOI8-U-specific Unicode conversion tree (koi8u_from_unicode_tree) to perform the actual character mapping from UTF-8 Unicode codepoints to KOI8-U byte sequences.

## Parameters / Member Variables
- : Source UTF-8 encoded string (null-terminated C string)
- : Destination buffer for KOI8-U encoded output (null-terminated C string)
- : Length of the source string in bytes
- : Error handling flag - if true, don't throw an error if conversion fails

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (parameter extraction)
  - PG_GETARG_INT32 (parameter extraction)
  - PG_GETARG_BOOL (parameter extraction)
  - CHECK_ENCODING_CONVERSION_ARGS (validation macro)
  - [UtfToLocal](../U/UtfToLocal.md) (core UTF-8 to local encoding conversion function)
  - PG_RETURN_INT32 (return value macro)
  - koi8u_from_unicode_tree (KOI8-U Unicode conversion table)
- Called from:
  - No direct references found (likely registered as encoding conversion procedure)

## Notes and Other Information
- This function follows PostgreSQL's standard conversion procedure interface, returning the number of bytes successfully converted
- Uses encoding constants PG_UTF8 and PG_KOI8U for validation
- Part of the utf8_and_cyrillic conversion module
- KOI8-U encoding is primarily used for Ukrainian text and extends KOI8-R with additional Ukrainian-specific Cyrillic characters
- The conversion relies on a Unicode tree structure for efficient character mapping
- Error handling is controlled by the noError parameter, allowing for graceful failure handling when requested
- KOI8-U includes characters like Ukrainian 'ґ' (ghe with upturn) and 'є' (Ukrainian ye) not present in KOI8-R