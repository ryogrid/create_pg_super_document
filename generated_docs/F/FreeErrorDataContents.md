# FreeErrorDataContents

## Location
src/backend/utils/error/elog.c: 1830 - 1866

## Overview
Frees all separately-allocated string fields within an ErrorData structure without deallocating the structure itself.

## Definition
```c
static void FreeErrorDataContents(ErrorData *edata)
```

## Detailed Description
FreeErrorDataContents is a static utility function that systematically deallocates all dynamically allocated string fields within an ErrorData structure. This function handles the cleanup of message text, details, hints, context information, backtrace data, and database object names (schema, table, column, datatype, constraint names) as well as internal query strings. Each field is null-checked before attempting to free it, making the function safe to call on partially initialized or already cleaned ErrorData structures. This function can be used on both error stack entries and copied ErrorData structures.

## Parameters / Member Variables
- `edata`: Pointer to the ErrorData structure whose contents should be freed

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (structure type)
  - pfree (memory deallocation function)

- Called from (representative examples):
  - errfinish
  - FreeErrorData

## Notes and Other Information
- Static function scope limits its usage to within the elog.c file
- Performs null checks on all fields before freeing to prevent segmentation faults
- Does not free the ErrorData structure itself, only its dynamically allocated contents
- Safe to call multiple times on the same ErrorData structure due to null checking
- Used both during normal error processing cleanup and when explicitly freeing copied ErrorData structures
- Covers all known dynamically allocated string fields in the ErrorData structure