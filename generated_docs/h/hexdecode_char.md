# hexdecode_char

## Location
[src/common/parse_manifest.c:900-917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/parse_manifest.c#L900-L917)

## Overview
A static utility function that converts a single hexadecimal character (0-9, a-f, A-F) to its corresponding integer value, returning -1 for invalid characters.

## Definition
```c
static int hexdecode_char(char c)
```

## Detailed Description
The `hexdecode_char` function performs hexadecimal character-to-integer conversion, handling both uppercase and lowercase hexadecimal digits. It serves as a fundamental building block for hexadecimal string parsing operations within the backup manifest parsing system. The function follows standard hexadecimal conventions where digits 0-9 map to values 0-9, letters a-f (or A-F) map to values 10-15, and any other character is considered invalid.

The function uses simple character arithmetic to perform the conversion efficiently, subtracting the appropriate ASCII base value from the input character to obtain the numeric result. This approach avoids lookup tables and provides fast conversion for valid hexadecimal characters.

## Parameters / Member Variables
- `c`: The character to be converted from hexadecimal representation to integer value

## Return Value
- Returns integer value 0-15 for valid hexadecimal characters (0-9, a-f, A-F)
- Returns -1 for invalid characters (not a hexadecimal digit)

## Dependencies
- Functions called/Symbols referenced: (none)

- Called from (representative examples):
  - [hexdecode_string](hexdecode_string.md) (primary caller for string conversion)
  - [JsonManifestParseIncrementalState](../J/JsonManifestParseIncrementalState.md) (structure reference)

## Notes and Other Information
- This is a static function, limiting its scope to the parse_manifest.c compilation unit
- Handles both uppercase (A-F) and lowercase (a-f) hexadecimal letters
- Uses character arithmetic rather than lookup tables for efficiency
- Serves as a building block for more complex hexadecimal string parsing operations
- The -1 return value provides a clear indication of invalid input for error handling
- Standard hexadecimal digit mapping: 0-9→0-9, a-f→10-15, A-F→10-15