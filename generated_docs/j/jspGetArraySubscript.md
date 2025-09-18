# jspGetArraySubscript

## Location
[src/backend/utils/adt/jsonpath.c:1231-1246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L1231-L1246)

## Overview
Extracts array subscript information from a JsonPathItem structure, initializing 'from' and 'to' JsonPathItem objects that represent the range bounds of an array index operation.

## Definition
```c
bool jspGetArraySubscript(JsonPathItem *v, JsonPathItem *from, JsonPathItem *to, int i)
```

## Detailed Description
This function extracts array subscript range information from a JsonPathItem of type jpiIndexArray. It processes the i-th element of the array subscript and initializes two output JsonPathItem structures: one for the 'from' bound and optionally one for the 'to' bound of a range operation.

The function always initializes the 'from' parameter with the starting bound of the subscript. If the subscript represents a range (has a 'to' component), it initializes the 'to' parameter as well and returns true. If it's a single index access (no 'to' component), it returns false to indicate that only the 'from' bound is valid.

Both output parameters are initialized using jspInitByBuffer, which sets up JsonPathItem structures to point to specific locations within the original buffer containing the JSON path data.

## Parameters / Member Variables
- `v`: JsonPathItem pointer of type jpiIndexArray containing the array subscript information
- `from`: Output JsonPathItem pointer to be initialized with the starting bound of the subscript
- `to`: Output JsonPathItem pointer to be initialized with the ending bound (if range operation)
- `i`: Index of the array subscript element to extract (0-based)

## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItem (structure type)
  - jpiIndexArray (enum constant)
  - [jspInitByBuffer](jspInitByBuffer.md) (function)
- Called from (representative examples):
  - [printJsonPathItem](../p/printJsonPathItem.md)
  - [jspIsMutableWalker](jspIsMutableWalker.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - jspHasNext

## Notes and Other Information
- The function uses Assert() to validate that the JsonPathItem type is jpiIndexArray
- Returns true if the subscript represents a range operation (has both 'from' and 'to'), false for single index access
- The initialized JsonPathItem structures share the same base buffer as the original JsonPathItem
- Used primarily for processing array access operations like [0], [1:3], or [:5] in JSON path expressions
- The 'i' parameter allows extraction of multiple subscript elements when an array access has multiple index specifications