# array_subscript_handler

## Location
src/backend/utils/adt/arraysubs.c: 539 - 565

## Overview
Provides the main subscripting handler for PostgreSQL's standard varlena arrays, returning a structure of function pointers that define array subscripting behavior.

## Definition
```c
Datum array_subscript_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the entry point for PostgreSQL's array subscripting framework for standard varlena arrays. It returns a pointer to a static SubscriptRoutines structure that defines the complete set of operations available for array subscripting, including transformation and execution setup functions. The handler is specifically designed for "true" array types that have proper array headers as understood by varlena array routines and are properly referenced by their element type's pg_type.typarray field. The returned structure specifies behavioral characteristics such as fetch strictness, leakproof properties, and error handling behavior.

## Parameters / Member Variables
- Standard PostgreSQL function arguments (PG_FUNCTION_ARGS macro)

## Dependencies
- Functions called/Symbols referenced:
  - SubscriptRoutines (struct)
  - [array_subscript_transform](array_subscript_transform.md)
  - [array_exec_setup](array_exec_setup.md)
- Called from (representative examples):
  - [CATALOG](../C/CATALOG.md) (pg_type system catalog)

## Notes and Other Information
- Returns a static SubscriptRoutines structure with predefined function pointers
- Configured with fetch_strict=true, meaning fetch returns NULL for NULL inputs
- Configured with fetch_leakproof=true, meaning fetch returns NULL for invalid subscripts
- Configured with store_leakproof=false, meaning assignment operations throw errors for invalid cases
- Only suitable for standard varlena arrays with proper array headers
- Must be referenced in the element type's pg_type.typarray field to function properly
- Part of PostgreSQL's pluggable subscripting architecture introduced to support custom subscripting behavior
- The function follows PostgreSQL's PG_FUNCTION_ARGS calling convention