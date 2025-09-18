# numerictypmodin

## Location
src/backend/utils/adt/numeric.c: 1322 - 1366

## Overview
The  function parses and validates type modifier strings for the NUMERIC data type, converting them into internal typmod representations.

## Definition


## Detailed Description
This function is part of PostgreSQL's type modifier input system for the NUMERIC type. It takes an array of type modifier values (typically precision and scale) from a type declaration like NUMERIC(10,2) and converts them into a single int32 typmod value used internally by the system. The function validates that precision and scale values are within acceptable ranges and handles both single-parameter (precision only) and dual-parameter (precision and scale) formats.

## Parameters / Member Variables
- : Array of type modifier values from the type declaration (PG_GETARG_ARRAYTYPE_P(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P: Extracts array argument from function call
  - [ArrayGetIntegerTypmods](../A/ArrayGetIntegerTypmods.md): Extracts integer values from the typmod array
  - NUMERIC_MAX_PRECISION: Maximum allowed precision constant
  - NUMERIC_MIN_SCALE: Minimum allowed scale constant  
  - NUMERIC_MAX_SCALE: Maximum allowed scale constant
  - ereport: Error reporting function
  - [errcode](../e/errcode.md): Error code specification
  - [errmsg](../e/errmsg.md): Error message formatting
  - [make_numeric_typmod](../m/make_numeric_typmod.md): Creates internal typmod representation
  - PG_RETURN_INT32: Returns int32 result

- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md): JSON path execution context

## Notes and Other Information
- Handles type declarations like NUMERIC(precision) and NUMERIC(precision, scale)
- Enforces PostgreSQL's limits on numeric precision and scale values
- Scale defaults to 0 when only precision is specified
- Validates precision must be between 1 and NUMERIC_MAX_PRECISION
- Validates scale must be between NUMERIC_MIN_SCALE and NUMERIC_MAX_SCALE
- Part of the type system's input/output machinery for custom types
- Located in src/backend/utils/adt/numeric.c:1322-1366