# raw_array_subscript_handler

## Location
src/backend/utils/adt/arraysubs.c: 566 - 577

## Overview
Subscripting handler function for "raw" arrays that contain N independent instances of the element type, providing the core functionality for array subscript operations in PostgreSQL.

## Definition
```c
Datum raw_array_subscript_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
The `raw_array_subscript_handler` is a specialized subscripting handler for "raw" arrays in PostgreSQL. A "raw" array is a simple array structure that contains N independent instances of the element type, without the overhead of varlena headers or complex storage mechanisms.

Key characteristics of raw arrays:
- Currently requires both the element type and the array type to be fixed length
- Contains independent instances of the element type without additional metadata
- Uses the same support code as standard varlena arrays for efficiency
- The main purpose of having a separate handler is to clearly indicate the array type semantics through `pg_type.typsubscript`

The function returns a static `SubscriptRoutines` structure that defines how subscript operations should be handled for raw arrays, including transformation, execution setup, and various behavioral flags.

## Parameters / Member Variables
This function takes standard PostgreSQL function arguments via `PG_FUNCTION_ARGS` macro but does not use any specific parameters.

The returned `SubscriptRoutines` structure contains:
- `transform`: Set to `array_subscript_transform` - handles parse analysis for subscript expressions
- `exec_setup`: Set to `array_exec_setup` - handles expression compilation
- `fetch_strict`: Set to `true` - fetch operations return NULL for NULL inputs
- `fetch_leakproof`: Set to `true` - fetch operations return NULL for bad subscripts (no error information leakage)
- `store_leakproof`: Set to `false` - assignment operations throw errors for invalid subscripts

## Dependencies
- Functions called/Symbols referenced:
  - `SubscriptRoutines` (structure type)
  - `[array_subscript_transform](../a/array_subscript_transform.md)` (transform function)
  - `[array_exec_setup](../a/array_exec_setup.md)` (execution setup function)
- Called from (representative examples):
  - No direct references found in the current codebase (likely referenced through PostgreSQL type system)

## Notes and Other Information
- The handler shares support code with standard varlena arrays, which may be split into separate code paths in the future for potential marginal speedups
- The primary benefit of having a separate handler is semantic clarity in the PostgreSQL type system
- Raw arrays are designed for efficiency when dealing with fixed-length element types
- The handler is registered in the PostgreSQL type system and called automatically during array subscript operations
- Location: `src/backend/utils/adt/arraysubs.c:566-577`