# SPI_getargcount

## Location
[src/backend/executor/spi.c:1890-1909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1890-L1909)

## Overview
Retrieve the number of parameters (arguments) that a prepared SPI plan expects.

## Definition

```c
int
SPI_getargcount(SPIPlanPtr plan)
```
## Detailed Description
SPI_getargcount returns the number of parameters that a prepared SPI plan expects when executed. This function is essential for determining how many arguments need to be provided when calling SPI_execute_plan or related execution functions. It performs validation to ensure the plan is valid by checking the magic number.

The function is commonly used in generic SPI utility functions that need to handle plans with varying numbers of parameters, or when implementing parameter validation routines.

## Parameters / Member Variables
- `plan`: An SPIPlanPtr pointing to a previously prepared SPI plan. Must be a valid, non-NULL plan.
## Dependencies
- Functions called/Symbols referenced:
  - [SPIPlanPtr](SPIPlanPtr.md) (typedef for struct _SPI_plan *)
  - _SPI_PLAN_MAGIC (validation constant)
  - SPI_ERROR_ARGUMENT (error code)
- Called from (representative examples):
  - Parameter validation routines
  - Generic SPI execution wrappers
  - Functions that need to iterate through plan parameters

## Notes and Other Information
- Returns -1 and sets SPI_result to SPI_ERROR_ARGUMENT if the plan is invalid or NULL
- The returned count can be 0 for plans that don't require parameters
- This function is read-only and does not modify the plan
- Often used in conjunction with SPI_getargtypeid to examine all plan parameters
- The plan must have been successfully prepared before calling this function
- Useful for bounds checking before calling SPI_getargtypeid with specific indices
- The returned value corresponds to the plan->nargs field in the internal _SPI_plan structure