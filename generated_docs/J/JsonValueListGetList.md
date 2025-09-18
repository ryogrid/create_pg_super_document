# JsonValueListGetList

## Location
src/backend/utils/adt/jsonpath_exec.c: 3545 - 3553

## Overview
Converts a JsonValueList structure into a standard PostgreSQL List, handling the conversion from singleton representation to list format when necessary.

## Definition
static List *JsonValueListGetList(JsonValueList *jvl)

## Detailed Description
This function returns a PostgreSQL List containing all the JsonbValue elements from a JsonValueList structure. If the JsonValueList contains a singleton value, it creates a new single-element list using list_make1(). If the JsonValueList already contains a list, it returns that list directly.

This function is essential for operations that need to iterate over all values in a JsonValueList or when interfacing with PostgreSQL's list-based APIs. It provides a unified interface to access all values regardless of the internal representation.

## Parameters / Member Variables
- jvl: Pointer to a JsonValueList structure to convert to a list format

## Dependencies
- Functions called/Symbols referenced:
  - [JsonValueList](JsonValueList.md) (structure type)
  - list_make1 (PostgreSQL list utility function)
- Called from (representative examples):
  - [jsonb_path_query_internal](../j/jsonb_path_query_internal.md)
  - RETURN_ERROR macro

## Notes and Other Information
- This is a static function internal to the jsonpath execution module
- Creates a new single-element list for singleton values, but returns the existing list for multi-value cases
- The caller should be aware that singleton cases return a newly allocated list
- Part of the JSON path expression evaluation system in PostgreSQL
- Provides a uniform interface for accessing all values in a JsonValueList
- Used primarily when all values need to be processed or returned as a query result
- The function maintains the optimization benefits of JsonValueList while providing list compatibility