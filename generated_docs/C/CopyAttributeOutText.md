# CopyAttributeOutText

## Location
src/backend/commands/copyto.c: 987 - 1139

## Overview
CopyAttributeOutText formats a string attribute for text-mode COPY TO output by applying proper escaping and encoding conversion as needed.

## Definition
```c
static void CopyAttributeOutText(CopyToState cstate, const char *string)
```

## Detailed Description
CopyAttributeOutText processes individual string attributes during text-format COPY TO operations by scanning the input string for characters that require escaping and applying appropriate transformations. The function handles encoding conversion when necessary, escapes control characters using C-like notation (\n, \r, \t, etc.), and escapes delimiter characters and backslashes. It uses an optimized approach with two different code paths depending on whether the encoding embeds ASCII characters, allowing for performance optimization in the common case of safe encodings. The function batches output using the DUMPSOFAR macro to minimize overhead from individual character operations.

## Parameters / Member Variables
- `cstate`: CopyToState structure containing formatting configuration and output settings
- `string`: Input string to be formatted and escaped for text output

## Dependencies
- Functions called/Symbols referenced:
  - [pg_server_to_any](../p/pg_server_to_any.md)
  - DUMPSOFAR
  - [CopySendChar](CopySendChar.md)
  - IS_HIGHBIT_SET
  - [pg_encoding_mblen](../p/pg_encoding_mblen.md)
- Called from (representative examples):
  - [DoCopyTo](../D/DoCopyTo.md)
  - [CopyOneRowTo](CopyOneRowTo.md)
  - DR_copy

## Notes and Other Information
The function includes performance optimizations by providing separate code paths for encodings that embed ASCII versus those that don't, avoiding unnecessary multibyte character length calculations in the safe case. Control characters are converted to their C-style escape sequences (\b, \f, \n, \r, \t, \v) for better readability and compatibility with various systems. The function handles delimiter characters and backslashes by prefixing them with backslashes. The DUMPSOFAR macro is used strategically to output accumulated characters in batches, reducing the number of individual send operations and improving performance for longer strings.