# shell_out

## Location
[src/backend/utils/adt/pseudotypes.c:313-337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudotypes.c#L313-L337)

## Overview
The shell_out function is an output function for PostgreSQL shell types that serves as a placeholder and error handler for incomplete type definitions.

## Definition
Datum shell_out(PG_FUNCTION_ARGS)

## Detailed Description
The shell_out function is designed to handle output operations on PostgreSQL shell types - these are type definitions that exist in pg_type but are not yet fully defined or implemented. The function acts as a safety mechanism that should theoretically never be reached during normal operation. When called, it immediately raises an error with ERRCODE_FEATURE_NOT_SUPPORTED, indicating that shell type values cannot be displayed. This prevents undefined behavior when code attempts I/O operations on incomplete type definitions without properly checking pg_type.typisdefined.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - ereport (PostgreSQL error reporting function)
  - PG_RETURN_VOID (macro for returning void datum, used to satisfy compiler)
- Called from (representative examples):
  - (No direct references found in codebase - should be unreachable)

## Notes and Other Information
- Part of PostgreSQL's pseudo-type system located in src/backend/utils/adt/pseudotypes.c
- Complements shell_in by providing the output counterpart for shell type safety
- Should be unreachable under normal PostgreSQL operation
- The error message "cannot display a value of a shell type" helps identify programming errors
- Works in conjunction with pg_type.typisdefined checks to ensure type system integrity
- The PG_RETURN_VOID at the end exists solely to keep the compiler quiet about return value expectations
- Serves as a defensive programming measure to prevent undefined behavior on incomplete type definitions

## Simplified Source

```c
Datum shell_out(PG_FUNCTION_ARGS) {
    // Error handler for shell types (incomplete type definitions)
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("cannot display a value of a shell type")));

    PG_RETURN_VOID();  // keep compiler quiet
}
```