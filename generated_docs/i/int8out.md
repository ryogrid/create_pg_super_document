# int8out

## Location
[src/backend/utils/adt/int8.c:61-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L61-L82)

## Overview
Converts PostgreSQL's internal int8 (bigint) value to its string representation for output.

## Definition


## Detailed Description
The  function serves as the output conversion routine for PostgreSQL's int8 data type (bigint). It takes a 64-bit integer from PostgreSQL's internal Datum representation and converts it to a C-string format suitable for display or transmission. This function is part of the PostgreSQL type system's input/output infrastructure and is automatically called when converting int8 values to text during query result formatting.

The function uses an optimized approach by pre-calculating the string length using  and then performing manual memory allocation and copying to avoid the overhead of  that would occur with .

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides access to:
  -  (int64): The 64-bit integer value to convert to string

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract int64 argument from function arguments
  - : Constant defining maximum length of int8 string representation
  - : Function to convert long long integer to string representation
  - : PostgreSQL memory allocation function
  - : Standard C memory copy function
  - : Macro to return C-string as Datum
- Called from (representative examples):
  - : Used in formatting functions for character conversion

## Notes and Other Information
- This function is registered in the PostgreSQL type system as the output function for the int8/bigint data type
- Uses an optimized memory allocation strategy to avoid unnecessary  calls
- The buffer size  accommodates the maximum possible digits plus null terminator for a 64-bit integer
- Memory allocated with  is automatically freed at the end of the current memory context
- Located in src/backend/utils/adt/int8.c alongside other 64-bit integer utility functions