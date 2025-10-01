# JsonValueListIsEmpty

## Location
[src/backend/utils/adt/jsonpath_exec.c:3533-3538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3533-L3538)

## Overview
Determines whether a JsonValueList structure contains no JSON values, checking both singleton and list representations for emptiness.

## Definition
static bool JsonValueListIsEmpty(JsonValueList *jvl)

## Detailed Description
This function checks if a JsonValueList structure is empty by examining both possible representations. A JsonValueList is considered empty when it has no singleton value (singleton is NULL) and its list pointer is NIL (indicating an empty or uninitialized list). This function is essential for conditional logic in JSON path execution where operations need to handle empty result sets differently.

The function efficiently determines emptiness by checking the two fields that define the state of a JsonValueList: the singleton pointer and the list pointer.

## Parameters / Member Variables
- jvl: Pointer to a JsonValueList structure to check for emptiness

## Dependencies
- Functions called/Symbols referenced:
  - [JsonValueList](JsonValueList.md) (structure type)
  - NIL (PostgreSQL constant for empty list)
- Called from (representative examples):
  - [executeJsonPath](../e/executeJsonPath.md)
  - [executeBoolItem](../e/executeBoolItem.md)
  - RETURN_ERROR macro

## Notes and Other Information
- This is a static function internal to the jsonpath execution module
- Returns true only when both singleton is NULL and list is NIL
- Used for error handling and conditional execution paths in JSON path operations
- Part of the JSON path expression evaluation system in PostgreSQL
- The function helps distinguish between empty results and results with actual values

## Simplified Source

```c
static bool JsonValueListIsEmpty(JsonValueList *jvl) {
    // Check if both singleton value and list are empty
    return !jvl->singleton && (jvl->list == NIL);
}
```