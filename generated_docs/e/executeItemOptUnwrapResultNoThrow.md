# executeItemOptUnwrapResultNoThrow

## Location
[src/backend/utils/adt/jsonpath_exec.c:1760-1776](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L1760-L1776)

## Overview
This function provides error-suppressed execution of JSONPath items with optional unwrapping, temporarily disabling error throwing during execution.

## Definition


## Detailed Description
The  function serves as a wrapper around  that provides error suppression capabilities. It temporarily modifies the execution context to disable error throwing, executes the JSONPath item with optional unwrapping, and then restores the original error handling state. This pattern allows for safe execution of JSONPath expressions in contexts where errors should be handled gracefully rather than propagated as exceptions.

## Parameters / Member Variables
- : JSONPath execution context containing state and configuration
- : JSONPath item representing the path expression to execute
- : JsonbValue containing the input data to process
- : Boolean flag indicating whether unwrapping should be attempted
- : List to collect final results after processing

## Dependencies
- Functions called/Symbols referenced:
  - [executeItemOptUnwrapResult](executeItemOptUnwrapResult.md)
  - [JsonPathExecContext](../J/JsonPathExecContext.md) (type)
  - JsonPathItem (type)
  - [JsonValueList](../J/JsonValueList.md) (type)
  - [JsonPathExecResult](../J/JsonPathExecResult.md) (return type)
- Called from (representative examples):
  - [executeBoolItem](executeBoolItem.md)
  - [executePredicate](executePredicate.md)

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c
- Implements the error suppression pattern by temporarily modifying the throwErrors flag in the execution context
- Ensures proper restoration of the original throwErrors state regardless of execution outcome
- Critical for boolean and predicate evaluations where errors should be converted to failure results rather than thrown as exceptions
- The function preserves the exact return value from executeItemOptUnwrapResult, allowing callers to distinguish between different execution results
- Used in contexts where JSONPath execution should fail gracefully without interrupting the broader operation