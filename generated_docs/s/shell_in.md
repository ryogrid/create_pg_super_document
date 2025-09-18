# shell_in

## Location
src/backend/utils/adt/pseudotypes.c: 303 - 312

## Overview
The shell_in function is an input function for PostgreSQL shell types that serves as a placeholder and error handler for incomplete type definitions.

## Definition
Datum shell_in(PG_FUNCTION_ARGS)

## Detailed Description
The shell_in function is designed to handle input operations on PostgreSQL shell types - these are type definitions that exist in pg_type but are not yet fully defined or implemented. The function acts as a safety mechanism that should theoretically never be reached during normal operation. When called, it immediately raises an error with ERRCODE_FEATURE_NOT_SUPPORTED, indicating that shell types cannot accept values. This prevents undefined behavior when code attempts I/O operations on incomplete type definitions without properly checking pg_type.typisdefined.

## Parameters / Member Variables
- Uses PostgreSQL's standard function argument macro PG_FUNCTION_ARGS which provides access to function call context

## Dependencies
- Functions called/Symbols referenced:
  - ereport (PostgreSQL error reporting function)
  - PG_RETURN_VOID (macro for returning void datum, used to satisfy compiler)
- Called from (representative examples):
  - (No direct references found in codebase - should be unreachable)

## Notes and Other Information
- Part of PostgreSQL's pseudo-type system located in src/backend/utils/adt/pseudotypes.c
- Designed as a defensive programming measure for shell type safety
- Should be unreachable under normal PostgreSQL operation
- The error message "cannot accept a value of a shell type" helps identify programming errors
- Works in conjunction with pg_type.typisdefined checks to ensure type system integrity
- The PG_RETURN_VOID at the end exists solely to keep the compiler quiet about return value expectations