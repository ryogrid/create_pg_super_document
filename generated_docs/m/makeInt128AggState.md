# makeInt128AggState

## Location
src/backend/utils/adt/numeric.c: 5496 - 5519

## Overview
Creates and initializes a state structure for 128-bit aggregate functions that compute sum, count, and optionally sum of squares of input values.

## Definition


## Detailed Description
This function prepares state data for 128-bit aggregate functions by allocating and initializing an  structure in the appropriate aggregate memory context. The function ensures proper memory management by switching to the aggregate context during allocation, which guarantees that the state persists for the duration of the aggregation operation. The state structure will track whether sum of squares calculation is needed based on the  parameter.

## Parameters / Member Variables
- : Function call information structure containing context about the aggregate function call
- : Boolean flag indicating whether the aggregate should calculate sum of squares in addition to sum and count

## Dependencies
- Functions called/Symbols referenced:
  - : Validates that the function is being called in an aggregate context
  - : Switches memory context for proper allocation
  - : Allocates zero-initialized memory
  - : The state structure type being allocated
- Called from (representative examples):
  - Used via  macro for polynomial numeric aggregates

## Notes and Other Information
- This is a static function, meaning it's only visible within the numeric.c file
- The function performs proper memory context management to ensure the state survives the aggregate operation
- There's a related function  that allocates in the current context instead
- The function is aliased as  through a macro definition
- Proper error handling ensures the function can only be called in aggregate contexts