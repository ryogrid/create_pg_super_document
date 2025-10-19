# btfloat4cmp

## Location
[src/backend/utils/adt/float.c:873-881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L873-L881)

## Overview
B-tree comparison function for single-precision floating-point numbers (float4) that provides three-way comparison for indexing operations.

## Definition

```c
Datum
btfloat4cmp(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the B-tree operator class comparison function for float4 (single-precision floating-point) data types. It extracts two float4 arguments from the function call context and delegates the actual comparison logic to the internal  function. The function returns an integer indicating the comparison result: negative if the first argument is less than the second, zero if they are equal, and positive if the first argument is greater than the second. This three-way comparison is essential for B-tree index operations including searching, insertion, and maintenance.

## Parameters / Member Variables
- Function uses  macro to access arguments:
  - First argument (index 0): First float4 value to compare
  - Second argument (index 1): Second float4 value to compare

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract float4 arguments from function call
  - : Internal comparison function that performs the actual comparison logic
  - : Macro to return int32 result from PostgreSQL function
  - : Single-precision floating-point data type

- Called from (representative examples):
  - B-tree index operations for float4 columns
  - Used indirectly through PostgreSQL's operator class system

## Notes and Other Information
- This function is part of PostgreSQL's B-tree operator class infrastructure for float4 data types
- The actual comparison logic is implemented in  for code reuse across different comparison contexts
- Located in 
- Returns a Datum-wrapped int32 value following PostgreSQL's function calling conventions

## Simplified Source

```c
Datum btfloat4cmp(PG_FUNCTION_ARGS) {
    // Extract the two float4 arguments from SQL call
    float4 arg1 = PG_GETARG_FLOAT4(0);
    float4 arg2 = PG_GETARG_FLOAT4(1);

    // Delegate to internal comparison function and return integer result
    // Returns: -1 if arg1 < arg2, 0 if equal, +1 if arg1 > arg2
    return PG_RETURN_INT32(float4_cmp_internal(arg1, arg2));
}
```