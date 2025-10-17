# FreeErrorData

## Location
[src/backend/utils/error/elog.c:1818-1829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1818-L1829)

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
  - [ErrorData](../E/ErrorData.md) (structure type)
  - [FreeErrorDataContents](FreeErrorDataContents.md) (cleanup function for contents)
  - [pfree](../p/pfree.md) (memory deallocation)

- Called from (representative examples):
  - [PLy_output](../P/PLy_output.md)
  - [PLy_commit](../P/PLy_commit.md)  
  - [PLy_rollback](../P/PLy_rollback.md)
  - [pltcl_elog](../p/pltcl_elog.md)
  - CHANGES_THRESHOLD

## Notes and Other Information
- Should only be used on ErrorData structures created by CopyErrorData
- Provides proper encapsulation by hiding the details of which fields need separate deallocation
- Must be called to prevent memory leaks when ErrorData copies are no longer needed
- The two-step process (FreeErrorDataContents + pfree) ensures both string contents and the structure itself are properly deallocated

## Simplified Source

```c
void
FreeErrorData(ErrorData *edata)
{
    // Free all separately-allocated string fields
    FreeErrorDataContents(edata);

    // Free the ErrorData structure itself
    pfree(edata);
}
```