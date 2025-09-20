# btfloat4fastcmp

## Location
[src/backend/utils/adt/float.c:882-890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L882-L890)

## Overview
Internal fast comparison function for single-precision floating-point numbers (float4) used by the sort support infrastructure for optimized sorting operations.

## Definition

```c
static int
btfloat4fastcmp(Datum x, Datum y, SortSupport ssup)
```
## Detailed Description
This static function provides an optimized comparison interface for float4 values within PostgreSQL's sort support framework. Unlike the general-purpose  function that follows the standard PostgreSQL function calling conventions, this function operates directly on Datum values and is designed for high-performance sorting scenarios. It extracts float4 values from the provided Datum arguments and delegates to  for the actual comparison logic. The function is part of PostgreSQL's sort support infrastructure that provides specialized, fast comparison functions for various data types to improve sorting performance.

## Parameters / Member Variables
- : First Datum containing a float4 value to compare
- : Second Datum containing a float4 value to compare  
- : SortSupport context (unused in this function but required by the interface)

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract float4 value from Datum
  - : Internal comparison function that performs the actual comparison logic
  - : Sort support context type
  - : Single-precision floating-point data type

- Called from (representative examples):
  - : Function that sets up sort support for float4 operations

## Notes and Other Information
- This is a static function, only accessible within the same translation unit (float.c)
- Designed for high-performance sorting scenarios where the overhead of PostgreSQL's standard function calling conventions would be detrimental
- The SortSupport parameter is unused but required by the sort support function interface
- Located in 
- Part of PostgreSQL's sort support optimization framework introduced to improve query performance