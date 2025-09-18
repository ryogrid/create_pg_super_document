# wrapItemsInArray

## Location
src/backend/utils/adt/jsonpath_exec.c: 3649 - 3665

## Overview
wrapItemsInArray is a static function that constructs a JSONB array by iterating through a JsonValueList and wrapping all items into a single JSON array structure.

## Definition
static JsonbValue *wrapItemsInArray(const JsonValueList *items)

## Detailed Description
This function takes a JsonValueList containing multiple JsonbValue items and constructs a new JSONB array that contains all those items as elements. It uses PostgreSQL's JSONB construction infrastructure with JsonbParseState to build the array incrementally. The function begins by pushing a WJB_BEGIN_ARRAY token, then iterates through all items in the list using JsonValueListIterator and JsonValueListNext, adding each item as an array element with WJB_ELEM tokens, and finally closes the array with WJB_END_ARRAY. This is commonly used in JSON path operations that need to collect multiple result values into a single array result.

## Parameters / Member Variables
- `items`: Pointer to a JsonValueList containing the items to be wrapped in an array (const, indicating read-only access)

## Dependencies
- Functions called/Symbols referenced:
  - [JsonValueList](../J/JsonValueList.md) (structure for holding lists of JsonbValues)
  - [JsonbParseState](../J/JsonbParseState.md) (PostgreSQL's JSONB construction state)
  - [JsonValueListIterator](../J/JsonValueListIterator.md) (iterator for traversing JsonValueList)
  - WJB_BEGIN_ARRAY (token for starting array construction)
  - [pushJsonbValue](../p/pushJsonbValue.md) (function to add elements during JSONB construction)
  - [JsonValueListInitIterator](../J/JsonValueListInitIterator.md) (function to initialize the iterator)
  - [JsonValueListNext](../J/JsonValueListNext.md) (function to get next item from iterator)
  - WJB_ELEM (token for array elements)
  - WJB_END_ARRAY (token for ending array construction)
- Called from (representative examples):
  - [jsonb_path_query_array_internal](../j/jsonb_path_query_array_internal.md) (for collecting query results)
  - [JsonPathQuery](../J/JsonPathQuery.md) (for query result aggregation)

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c, not exposed in the public API
- Creates a new JSONB array structure containing all items from the input list
- Uses PostgreSQL's standard JSONB construction pattern with JsonbParseState
- The function returns the completed JsonbValue representing the constructed array
- Essential for JSON path operations that return multiple values as arrays
- Memory management for the constructed array is handled by the JSONB construction infrastructure
- The resulting array preserves the order of items as they appear in the input JsonValueList