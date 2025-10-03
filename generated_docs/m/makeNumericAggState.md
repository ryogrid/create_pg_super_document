# makeNumericAggState

## Location
[src/backend/utils/adt/numeric.c:4833-4857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4833-L4857)

## Overview
Creates and initializes a NumericAggState structure for numeric aggregate functions that need to compute sum, count, and optionally sum of squares of input values.

## Definition

```c
static NumericAggState *
makeNumericAggState(FunctionCallInfo fcinfo, bool calcSumX2)
```
## Detailed Description
This static function creates a properly initialized NumericAggState structure within the correct memory context for aggregate operations. It first validates that the function is being called in an appropriate aggregate context using . The function then switches to the aggregate memory context to allocate the state structure, ensuring the state persists for the duration of the aggregate operation. The allocated structure is zero-initialized using  and configured with the specified settings for sum-of-squares calculation and memory context reference.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing context and metadata for the aggregate function
- `calcSumX2`: Boolean flag indicating whether the aggregate should calculate sum of squares in addition to sum and count
## Dependencies
- Functions called/Symbols referenced:
  -  - Validate aggregate context and get memory context
  -  - Log error messages
  -  - Switch memory contexts
  -  - Allocate zero-initialized memory
  -  - Structure type for aggregate state
- Called from (representative examples):
  -  - Standard numeric accumulation function
  -  - [Numeric](../N/Numeric.md) average accumulation function  
  -  - Polymorphic numeric aggregate state creation
  -  - 64-bit integer accumulation function

## Notes and Other Information
- Declared as static, limiting visibility to numeric.c file
- Performs proper memory context management for aggregate operations
- Throws ERROR if called outside aggregate context to prevent misuse
- Uses  to ensure all fields are properly initialized to zero
- The state structure includes a reference to the aggregate memory context for later use
- Essential for PostgreSQL's aggregate function infrastructure
- Located in src/backend/utils/adt/numeric.c:4833-4857