# JsonValueListHead

## Location
[src/backend/utils/adt/jsonpath_exec.c:3539-3544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3539-L3544)

## Overview
Retrieves the first JSON value from a JsonValueList structure, handling both singleton and list representations efficiently.

## Definition
static JsonbValue *JsonValueListHead(JsonValueList *jvl)

## Detailed Description
This function returns a pointer to the first JsonbValue in a JsonValueList structure. It efficiently handles the dual representation of JsonValueList by first checking if the structure contains a singleton value (returning it directly) or accessing the first element of the underlying linked list using PostgreSQL's linitial() function.

This function is commonly used when operations need to access the first result from a JSON path query, particularly in scenarios where only the first value is needed or when validating single-value results.

## Parameters / Member Variables
- jvl: Pointer to a JsonValueList structure from which to retrieve the first value

## Dependencies
- Functions called/Symbols referenced:
  - [JsonValueList](JsonValueList.md) (structure type)
  - linitial (PostgreSQL list utility function)
- Called from (representative examples):
  - [jsonb_path_match_internal](../j/jsonb_path_match_internal.md)
  - [jsonb_path_query_first_internal](../j/jsonb_path_query_first_internal.md)
  - [executeBinaryArithmExpr](../e/executeBinaryArithmExpr.md)
  - [getArrayIndex](../g/getArrayIndex.md)
  - [JsonPathQuery](JsonPathQuery.md)
  - [JsonPathValue](JsonPathValue.md)

## Notes and Other Information
- This is a static function internal to the jsonpath execution module
- Returns the singleton value directly if present, otherwise accesses the first list element
- Assumes the JsonValueList is not empty - callers should verify using JsonValueListIsEmpty if needed
- Part of the JSON path expression evaluation system in PostgreSQL
- Optimized for the common case of single-value results through the singleton representation
- Used extensively in operations that need to extract scalar values from JSON path results

## Simplified Source

```c
static JsonbValue *
JsonValueListHead(JsonValueList *jvl)
{
    // Return singleton value if present, otherwise first list element
    return jvl->singleton ? jvl->singleton : linitial(jvl->list);
}
```