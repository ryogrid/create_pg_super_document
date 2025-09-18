# CopySendChar

## Location
src/backend/commands/copyto.c: 181 - 186

## Overview
CopySendChar is a static function that appends a single character to the frontend message buffer during COPY TO operations, providing an efficient method for transmitting individual character data.

## Definition
```c
static void CopySendChar(CopyToState cstate, char c)
```

## Detailed Description
This function is a specialized convenience function that appends a single character to the frontend message buffer during copy operations. It uses the appendStringInfoCharMacro for efficient single-character appending, which is optimized for this common operation in text formatting. The function is particularly useful for sending delimiter characters, escape sequences, quotes, and other single-character formatting elements in text-based copy operations. Like other copy send functions, it buffers the data without immediate transmission to the client.

## Parameters / Member Variables
- `cstate`: Pointer to CopyToState structure containing the state information for the copy operation, including the frontend message buffer where the character will be appended
- `c`: The single character to be sent to the client

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoCharMacro (optimized macro for appending a single character to the message buffer)
- Called from (representative examples):
  - DR_copy (in copyto.c:122)
  - CopySendEndOfRow (in copyto.c:198, 240)
  - DoCopyTo (in copyto.c:835)
  - CopyOneRowTo (in copyto.c:936)
  - CopyAttributeOutText (in copyto.c:1057, 1058, 1064, 1117, 1118, 1124)
  - CopyAttributeOutCSV (in copyto.c:1192, 1203, 1213)

## Notes and Other Information
- Uses appendStringInfoCharMacro for optimized single-character appending performance
- Commonly used for sending delimiter characters (tabs, commas), quotes, and escape sequences
- This function is static, meaning it's only accessible within the copyto.c file
- Data is buffered and not immediately sent to maintain efficiency
- Frequently used in text formatting functions for CSV and text output formats
- The function is heavily utilized throughout the copy formatting process as evidenced by its many call sites