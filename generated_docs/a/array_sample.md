# array_sample

## Location
[src/backend/utils/adt/array_userfuncs.c:1660-1687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L1660-L1687)

## Overview
Returns an array containing n randomly chosen first-dimension elements from the input array using sampling without replacement.

## Definition

```c
Datum
array_sample(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements a PostgreSQL built-in function that performs random sampling from the first dimension of an array. It takes an input array and a sample size n, then returns a new array containing n randomly selected elements from the first dimension of the input array. The sampling is performed without replacement, meaning each element can only appear once in the result.

The function validates that the sample size is within valid bounds (between 0 and the number of elements in the first dimension) and uses the  helper function to perform the actual sampling operation. It maintains type information through PostgreSQL's type cache system for efficient processing.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (ArrayType*): The input array from which to sample elements
  -  (int32): The number of elements to sample from the array's first dimension

## Dependencies
- Functions called/Symbols referenced:
  - : Extract array argument from function call
  - : Extract integer argument from function call  
  - : Get number of dimensions of array
  - : Get dimension sizes of array
  - : Get element type of array
  - : Look up type information in PostgreSQL's type cache
  - : Perform the actual random sampling operation
  - : Return array result from function
- Called from (representative examples):
  - No direct callers found (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
- The function performs bounds checking to ensure the sample size n is between 0 and the number of elements in the first dimension
- Uses PostgreSQL's type cache system () for efficient type handling across multiple calls
- Relies on the  helper function to perform the actual random selection
- Returns an error with code  if the sample size is out of bounds
- The function operates only on the first dimension of multi-dimensional arrays
- Sampling is performed without replacement, ensuring no duplicate elements in the result