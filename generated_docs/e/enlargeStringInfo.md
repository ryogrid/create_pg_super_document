# enlargeStringInfo

## Location
[src/common/stringinfo.c:289-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/stringinfo.c#L289-L360)

## Overview
Enlarges a StringInfo buffer to ensure it has enough space for additional bytes beyond its current content.

## Definition

```c
void
enlargeStringInfo(StringInfo str, int needed)
```
## Detailed Description
This function ensures that a StringInfo buffer has sufficient space to accommodate 'needed' additional bytes beyond its current length. The function implements an exponential growth strategy, doubling the buffer size on each expansion to minimize the number of memory reallocations.

The function performs several critical safety checks:
- Validates that the StringInfo is not read-only (maxlen != 0)
- Guards against negative or overflow-causing 'needed' values
- Ensures the total required size doesn't exceed MaxAllocSize

If expansion is needed, the buffer size is doubled repeatedly until it can accommodate the required space. The buffer remains allocated in the same memory context where initStringInfo was originally called, which is critical for proper memory management in PostgreSQL's context-based allocation system.

External callers typically don't need to call this function directly since all stringinfo.c routines handle buffer expansion automatically. However, pre-enlarging the buffer can save palloc overhead when the final size is known in advance.

## Parameters / Member Variables
- : The StringInfo structure to enlarge
- : Number of additional bytes required (excluding the terminating null byte)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (validation macro)
  - elog/ereport (error reporting - backend)
  - fprintf/exit (error handling - frontend)
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - MaxAllocSize (maximum allocation size constant)
- Called from (representative examples):
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)  
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - [pq_getmessage](../p/pq_getmessage.md)
  - [JsonbToCStringWorker](../J/JsonbToCStringWorker.md)

## Notes and Other Information
- Uses exponential growth strategy (doubling) for efficiency
- Maintains memory context consistency with original initStringInfo call
- Different error handling between backend (elog/ereport) and frontend (fprintf/exit)
- Includes comprehensive overflow protection
- The 'needed' parameter does not include space for the terminating null byte
- Buffer expansion is automatic in most stringinfo.c operations

## Simplified Source

```c
// Simplified version of enlargeStringInfo
void
enlargeStringInfo(StringInfo str, int needed)
{
    // Validate StringInfo is not read-only
    Assert(str->maxlen != 0);

    // Validate request size
    if (needed < 0)
        elog(ERROR, "invalid string enlargement request size: %d", needed);

    // Check for potential overflow
    if (((Size) needed) >= (MaxAllocSize - (Size) str->len))
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("out of memory")));

    // Calculate total space required (including null terminator)
    int total_needed = needed + str->len + 1;

    // Return if we already have enough space
    if (total_needed <= str->maxlen)
        return;

    // Calculate new buffer size using exponential growth
    int newlen = 2 * str->maxlen;
    while (total_needed > newlen)
        newlen = 2 * newlen;

    // Clamp to maximum allocation size
    if (newlen > (int) MaxAllocSize)
        newlen = (int) MaxAllocSize;

    // Reallocate buffer and update maxlen
    str->data = (char *) repalloc(str->data, newlen);
    str->maxlen = newlen;
}
```

Key simplifications made:
- Consolidated frontend/backend error handling to backend version for clarity
- Added explanatory comments for each major step
- Simplified variable names and calculations
- Grouped related validation checks together
- Preserved essential logic: validate input, check overflow, calculate new size with exponential growth, reallocate
- Maintained the critical exponential growth strategy and overflow protection