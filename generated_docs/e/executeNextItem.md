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
  - jspGetNext
  - executeItem
  - JsonValueListAppend
  - copyJsonbValue
  - jperOk (enum value)
  - JsonPathExecContext (type)
  - JsonPathItem (type)
  - JsonValueList (type)
  - JsonPathExecResult (return type)
- Called from (representative examples):
  - executeItemOptUnwrapTarget
  - executeBinaryArithmExpr
  - executeUnaryArithmExpr
  - executeNumericItemMethod
  - executeDateTimeMethod
  - executeKeyValueMethod
  - appendBoolResult

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c
- The function handles three scenarios: explicit next item provided, current item with potential next, or no current item
- Uses jspHasNext and jspGetNext utility functions to navigate the JSONPath item chain
- Implements conditional value copying based on the copy parameter to optimize memory usage
- Returns jperOk when successfully completing the chain without finding a next item to execute
- Central to the sequential execution model of JSONPath expressions