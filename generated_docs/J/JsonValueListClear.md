# JsonValueListClear

## Location
src/backend/utils/adt/jsonpath_exec.c: 3506 - 3512

## Overview
Clears a JsonValueList structure by resetting both its singleton pointer and list to their initial empty states.

## Definition
```c
static void JsonValueListClear(JsonValueList *jvl)
```

## Detailed Description
The JsonValueListClear function resets a JsonValueList structure to its initial empty state. It sets the singleton pointer to NULL and the list field to NIL (PostgreSQL's empty list constant). This function is used to reinitialize JsonValueList structures for reuse, ensuring they start from a clean state without any residual values from previous operations.

## Parameters / Member Variables
- `jvl`: Pointer to the JsonValueList structure to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - [JsonValueList](JsonValueList.md) (structure type)
  - NIL (PostgreSQL empty list constant)
- Called from (representative examples):
  - [JsonTableResetRowPattern](JsonTableResetRowPattern.md) (row pattern reset operations)

## Notes and Other Information
- This is a simple utility function that performs a complete reset of JsonValueList state
- The JsonValueList structure uses two storage mechanisms: a singleton pointer for single values and a list for multiple values
- Setting both fields ensures the structure is completely cleared regardless of how it was previously used
- Part of the JsonValueList management system used throughout JSONPath execution
- Essential for proper memory management and state reset in JSON table operations and pattern matching