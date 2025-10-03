# resetStringInfo

## Location
[src/common/stringinfo.c:78-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/stringinfo.c#L78-L96)

## Overview
Resets a StringInfo to an empty state while preserving the allocated data buffer, clearing any previous content.

## Definition

```c
void
resetStringInfo(StringInfo str)
```
## Detailed Description
The  function clears the content of a StringInfo structure without deallocating its data buffer. It sets the string length to zero, null-terminates the string at position 0, and resets the cursor position to the beginning. The function includes an assertion to prevent resetting read-only StringInfos (those with maxlen == 0). This is an efficient way to reuse a StringInfo for new content without the overhead of memory deallocation and reallocation.

## Parameters / Member Variables
- `str`: Pointer to the StringInfo structure to be reset
## Dependencies
- Functions called/Symbols referenced:
  - Assert (debugging assertion)
- Called from (representative examples):
  - [CopyReadLine](../C/CopyReadLine.md)
  - [CopyReadAttributesText](../C/CopyReadAttributesText.md)  
  - [CopySendEndOfRow](../C/CopySendEndOfRow.md)
  - [pq_getmessage](../p/pq_getmessage.md)
  - [pq_beginmessage_reuse](../p/pq_beginmessage_reuse.md)
  - [WalSndPrepareWrite](../W/WalSndPrepareWrite.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [json_lex](../j/json_lex.md)

## Notes and Other Information
- This function is located at src/common/stringinfo.c:78-96
- Does not deallocate the data buffer, making it efficient for reuse scenarios
- Contains an assertion that prevents resetting read-only StringInfos
- Sets str->data[0] = '\0', str->len = 0, and str->cursor = 0
- Widely used in PostgreSQL for message processing, replication, and data parsing where StringInfo objects are reused multiple times
- Maintains the maxlen field unchanged, preserving the allocated buffer size

## Simplified Source

```c
// Simplified version of resetStringInfo
void resetStringInfo(StringInfo str) {
    // Clear the string content by null-terminating at the beginning
    str->data[0] = '\0';

    // Reset length and cursor to indicate empty string
    str->len = 0;
    str->cursor = 0;
}
```

Key simplifications made:
- Removed assertion check for read-only StringInfos for clarity
- Added descriptive comments explaining each step
- Focused on the core functionality of clearing the string content