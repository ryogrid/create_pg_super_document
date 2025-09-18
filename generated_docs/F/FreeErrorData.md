# FreeErrorData

## Location
src/backend/utils/error/elog.c: 1818 - 1829

## Overview
Safely deallocates an ErrorData structure previously created by CopyErrorData, ensuring all separately-allocated fields are properly freed.

## Definition
```c
void FreeErrorData(ErrorData *edata)
```

## Detailed Description
FreeErrorData is the proper cleanup function for ErrorData structures created by CopyErrorData. This function ensures complete deallocation by first calling FreeErrorDataContents to free all separately-allocated string fields within the ErrorData structure, then freeing the ErrorData structure itself. Error handlers should use this function rather than attempting to manually free individual fields, as it encapsulates knowledge of the complete structure layout and prevents memory leaks.

## Parameters / Member Variables
- `edata`: Pointer to the ErrorData structure to be freed (must have been allocated by CopyErrorData)

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (structure type)
  - FreeErrorDataContents (cleanup function for contents)
  - pfree (memory deallocation)

- Called from (representative examples):
  - PLy_output
  - PLy_commit  
  - PLy_rollback
  - pltcl_elog
  - CHANGES_THRESHOLD

## Notes and Other Information
- Should only be used on ErrorData structures created by CopyErrorData
- Provides proper encapsulation by hiding the details of which fields need separate deallocation
- Must be called to prevent memory leaks when ErrorData copies are no longer needed
- The two-step process (FreeErrorDataContents + pfree) ensures both string contents and the structure itself are properly deallocated