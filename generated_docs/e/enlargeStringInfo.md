# enlargeStringInfo

## Location
src/common/stringinfo.c: 289 - 360

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
  - repalloc (memory reallocation)
  - MaxAllocSize (maximum allocation size constant)
- Called from (representative examples):
  - appendStringInfo
  - appendStringInfoChar  
  - appendBinaryStringInfo
  - pq_getmessage
  - JsonbToCStringWorker

## Notes and Other Information
- Uses exponential growth strategy (doubling) for efficiency
- Maintains memory context consistency with original initStringInfo call
- Different error handling between backend (elog/ereport) and frontend (fprintf/exit)
- Includes comprehensive overflow protection
- The 'needed' parameter does not include space for the terminating null byte
- Buffer expansion is automatic in most stringinfo.c operations