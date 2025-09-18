# JsonPathExecResult

## Location
src/backend/utils/adt/jsonpath_exec.c: 142 - 143

## Overview
JsonPathExecResult is an enumeration that represents the possible execution results when evaluating JSONPath expressions in PostgreSQL.

## Definition
```c
typedef enum JsonPathExecResult
{
    jperOk = 0,
    jperNotFound = 1,
    jperError = 2
} JsonPathExecResult;
```

## Detailed Description
JsonPathExecResult serves as a standardized return type for JSONPath execution functions in PostgreSQL. It provides three distinct states that indicate the outcome of a JSONPath expression evaluation:

- **jperOk (0)**: Indicates successful execution of the JSONPath expression with valid results
- **jperNotFound (1)**: Indicates that the JSONPath expression executed successfully but did not find any matching values in the target JSON data
- **jperError (2)**: Indicates that an error occurred during JSONPath expression execution

This enum is extensively used throughout the JSONPath execution system to provide consistent error handling and result status reporting. The system uses this return value to determine whether to throw errors, return empty results, or continue processing depending on the execution context.

## Parameters / Member Variables
- `jperOk`: Successful execution with results found
- `jperNotFound`: Successful execution but no matching results
- `jperError`: Error occurred during execution

## Dependencies
- Used extensively by JSONPath execution functions including:
  - [executeJsonPath](../e/executeJsonPath.md)
  - [executeItem](../e/executeItem.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [executeBoolItem](../e/executeBoolItem.md)
  - [JsonPathExists](JsonPathExists.md)
  - [JsonPathQuery](JsonPathQuery.md)
  - [JsonPathValue](JsonPathValue.md)
- Works in conjunction with:
  - `RETURN_ERROR` macro for error handling
  - `jperIsError` macro for error checking
  - JsonPath execution context structures

## Notes and Other Information
- The enum values are designed to be compatible with boolean logic (0 = success, non-zero = failure/special case)
- A convenience macro `jperIsError(jper)` is provided to check if the result represents an error condition
- The `RETURN_ERROR` macro uses `jperError` as its return value when not throwing exceptions
- This enum is central to PostgreSQL's JSONPath implementation error handling strategy
- The three-state design allows differentiation between successful operations with no results versus actual execution errors