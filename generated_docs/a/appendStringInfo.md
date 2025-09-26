# appendStringInfo

## Location
src/common/stringinfo.c: 97 - 138

## Overview
Formats text data using printf-style formatting and appends it to an existing StringInfo, automatically handling buffer resizing as needed.

## Definition

```c
void
appendStringInfo(StringInfo str, const char *fmt,...)
```
## Detailed Description
The  function provides printf-style formatted text appending to a StringInfo structure. It uses variable arguments (variadic function) to accept format strings and parameters similar to sprintf. The function implements a retry loop that attempts to format the data and, if the buffer is too small, automatically enlarges it using  and tries again. This ensures that formatted text is always successfully appended regardless of the current buffer size. The function preserves the original errno value to avoid interfering with error handling in calling code.

## Parameters / Member Variables
- : Pointer to the StringInfo structure to append to
- : printf-style format string
- : Variable arguments corresponding to format specifiers in fmt

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoVA (actual formatting implementation)
  - enlargeStringInfo (buffer expansion)
- Called from (representative examples):
  - (No direct references found in the current analysis)

## Notes and Other Information
- This function is located at src/common/stringinfo.c:97-138
- Uses a retry loop to handle insufficient buffer space automatically
- Preserves errno across the operation by saving and restoring it
- Combines the functionality of sprintf and strcat in a single operation
- The actual formatting work is delegated to 
- Part of PostgreSQL's dynamic string building infrastructure, commonly used for constructing SQL queries, error messages, and formatted output
- Automatically manages memory allocation, making it safer than manual string manipulation