# SPI_getargtypeid

## Location
src/backend/executor/spi.c: 1875 - 1889

## Overview
Retrieve the type identifier (Oid) for a specific parameter of a prepared SPI plan.

## Definition


## Detailed Description
SPI_getargtypeid is used to examine the parameter types of a prepared SPI plan. Given a valid SPI plan and an argument index, it returns the PostgreSQL type Oid (Object identifier) for that parameter. This function is useful for introspection of plan parameters, especially when implementing generic functions that need to handle different parameter types dynamically.

The function performs validation to ensure the plan is valid (checking the magic number) and that the argument index is within the valid range. If any validation fails, it sets SPI_result to SPI_ERROR_ARGUMENT and returns InvalidOid.

Parameters are indexed starting from 0, so the first parameter is at index 0, the second at index 1, and so on.

## Parameters / Member Variables
- : An SPIPlanPtr pointing to a previously prepared SPI plan. Must be a valid, non-NULL plan.
- : Zero-based index of the parameter whose type is requested. Must be >= 0 and < plan->nargs.

## Dependencies
- Functions called/Symbols referenced:
  - [SPIPlanPtr](SPIPlanPtr.md) (typedef for struct _SPI_plan *)
  - _SPI_PLAN_MAGIC (validation constant)
  - SPI_ERROR_ARGUMENT (error code)
  - InvalidOid (constant representing invalid Oid)
- Called from (representative examples):
  - User-defined functions needing parameter type introspection
  - Generic SPI utility functions

## Notes and Other Information
- Returns InvalidOid and sets SPI_result to SPI_ERROR_ARGUMENT if validation fails
- The function accesses the plan->argtypes array directly after validation
- Parameter indexing is zero-based (first parameter is index 0)
- The returned Oid can be used with PostgreSQL's type system functions to get more detailed type information
- This function is read-only and does not modify the plan
- Commonly used in conjunction with SPI_getargcount to iterate through all plan parameters
- The plan must have been successfully prepared before calling this function