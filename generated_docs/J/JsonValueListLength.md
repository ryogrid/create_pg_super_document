# JsonValueListLength

## Location
[src/backend/utils/adt/jsonpath_exec.c:3527-3532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3527-L3532)

## Overview
Returns the number of JSON values stored in a JsonValueList structure, providing an efficient count operation that handles both singleton and list representations.

## Definition

```c
static int
JsonValueListLength(const JsonValueList *jvl)
```
## Detailed Description
This function calculates and returns the total number of JSON values contained within a JsonValueList structure. The JsonValueList is an optimization structure that can store either a single value (singleton) or multiple values in a linked list. The function efficiently determines the count by checking if the list is in singleton mode (returning 1) or by calling list_length() on the underlying list structure.

This function is used throughout the JSON path execution engine to determine the size of result sets and to validate operations that depend on the number of values present.

## Parameters / Member Variables
- : Pointer to a const JsonValueList structure whose length is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (PostgreSQL list utility function)
  - [JsonValueList](JsonValueList.md) (structure type)
- Called from (representative examples):
  - [jsonb_path_match_internal](../j/jsonb_path_match_internal.md)
  - [jsonb_path_query_first_internal](../j/jsonb_path_query_first_internal.md)
  - [executeBinaryArithmExpr](../e/executeBinaryArithmExpr.md)
  - [getArrayIndex](../g/getArrayIndex.md)
  - [JsonPathQuery](JsonPathQuery.md)
  - [JsonPathValue](JsonPathValue.md)

## Notes and Other Information
- This is a static function internal to the jsonpath execution module
- The function handles the dual representation efficiently: singleton values return 1 immediately without list traversal
- Part of the JSON path expression evaluation system in PostgreSQL
- Used for both validation (ensuring single values where required) and iteration control
- The JsonValueList structure uses a singleton optimization to avoid list overhead for single values

## Simplified Source

```c
static int
JsonValueListLength(const JsonValueList *jvl)
{
    // Return 1 for singleton, otherwise get list length
    return jvl->singleton ? 1 : list_length(jvl->list);
}
```