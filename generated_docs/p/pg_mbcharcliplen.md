# pg_mbcharcliplen

## Location
src/backend/utils/mb/mbutils.c: 1125 - 1149

## Overview
Clips a multi-byte string to a specified character limit (not byte limit), ensuring the result remains valid by not breaking multi-byte character boundaries.

## Definition
```c
int pg_mbcharcliplen(const char *mbstr, int len, int limit)
```

## Detailed Description
This function calculates the maximum number of bytes that can be taken from a multi-byte string to include up to a specified number of characters, without breaking multi-byte character boundaries. Unlike pg_mbcliplen and pg_encoding_mbcliplen which work with byte limits, this function works with character limits, making it useful for operations that need to limit the number of displayable characters rather than storage bytes.

The function iterates through the string character by character, counting both characters and accumulating byte lengths until the character limit is reached. For single-byte encodings, it optimizes by calling the simpler cliplen function directly since character count equals byte count.

## Parameters / Member Variables
- `mbstr`: Pointer to the input multi-byte string to be clipped
- `len`: The length of the input string in bytes
- `limit`: The maximum number of characters (not bytes) allowed in the result

## Dependencies
- Functions called/Symbols referenced:
  - [pg_database_encoding_max_length](pg_database_encoding_max_length.md)
  - [cliplen](../c/cliplen.md)
  - [pg_mblen](pg_mblen.md)
- Called from (representative examples):
  - [bpchar_input](../b/bpchar_input.md)
  - [bpchar](../b/bpchar.md)
  - [varchar_input](../v/varchar_input.md)
  - [varchar](../v/varchar.md)
  - [text_left](../t/text_left.md)
  - [text_right](../t/text_right.md)

## Notes and Other Information
- The function assumes the input string is valid in the database encoding
- For single-byte encodings, it delegates to the more efficient cliplen function
- The function counts characters, not bytes, making it suitable for user-visible string operations
- Widely used in PostgreSQL's string data type implementations (varchar, bpchar, text functions)
- Returns the actual number of bytes needed to represent the specified number of characters