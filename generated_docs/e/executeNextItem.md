# executeNextItem

## Location
src/backend/utils/adt/jsonpath_exec.c: 1694 - 1724

## Overview
This function executes the next JSONPath item if it exists, or adds the current value to the found list if there are no more items to process.

## Definition


## Detailed Description
The  function serves as a control flow manager in JSONPath execution. It determines whether there are more JSONPath items to process in the current expression chain. If a next item exists, it delegates execution to . If no next item exists and a results collection list is provided, it appends the current value to that list. This function implements the sequential processing logic that allows complex JSONPath expressions to be evaluated step by step.

## Parameters / Member Variables
- : JSONPath execution context containing state and configuration
- : Current JSONPath item being processed (can be NULL)
- : Next JSONPath item to process (can be NULL)
- : Current JsonbValue being processed
- : List to collect matching values (can be NULL)
- : Boolean flag indicating whether to copy the value when adding to found list

## Dependencies
- Functions called/Symbols referenced:
  - jspHasNext
  - [jspGetNext](../j/jspGetNext.md)
  - [executeItem](executeItem.md)
  - [JsonValueListAppend](../J/JsonValueListAppend.md)
  - [copyJsonbValue](../c/copyJsonbValue.md)
  - jperOk (enum value)
  - [JsonPathExecContext](../J/JsonPathExecContext.md) (type)
  - JsonPathItem (type)
  - [JsonValueList](../J/JsonValueList.md) (type)
  - [JsonPathExecResult](../J/JsonPathExecResult.md) (return type)
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md)
  - [executeBinaryArithmExpr](executeBinaryArithmExpr.md)
  - [executeUnaryArithmExpr](executeUnaryArithmExpr.md)
  - [executeNumericItemMethod](executeNumericItemMethod.md)
  - [executeDateTimeMethod](executeDateTimeMethod.md)
  - [executeKeyValueMethod](executeKeyValueMethod.md)
  - [appendBoolResult](../a/appendBoolResult.md)

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c
- The function handles three scenarios: explicit next item provided, current item with potential next, or no current item
- Uses jspHasNext and jspGetNext utility functions to navigate the JSONPath item chain
- Implements conditional value copying based on the copy parameter to optimize memory usage
- Returns jperOk when successfully completing the chain without finding a next item to execute
- Central to the sequential execution model of JSONPath expressions