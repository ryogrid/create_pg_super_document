# ConditionError

## Location
[src/bin/pgbench/pgbench.c:5881-5890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5881-L5890)

## Overview
Reports fatal errors related to conditional statement processing in pgbench scripts.

## Definition
static void ConditionError(const char *desc, int cmdn, const char *msg)

## Detailed Description
This is a specialized error reporting function for conditional statement errors in pgbench benchmark scripts. It formats and displays a fatal error message that includes the script description, command number, and specific error message, then terminates the program. The function is specifically designed to handle errors in if/elif/else/endif conditional constructs.

## Parameters / Member Variables
- desc: Description or name of the script being processed
- cmdn: Command number where the error occurred (for pinpointing location)
- msg: Specific error message describing what went wrong

## Dependencies
- Functions called/Symbols referenced:
  - [pg_fatal](../p/pg_fatal.md) (implicitly terminates program)
- Called from:
  - [CheckConditional](CheckConditional.md) (multiple calls at src/bin/pgbench/pgbench.c:5910, 5912, 5916, 5918, 5923, 5932)

## Notes and Other Information
- This is a fatal error function that does not return to caller
- Part of pgbench's conditional statement validation system
- Provides structured error reporting for script debugging
- The error message format includes script name and command number for precise error location
- Used exclusively by CheckConditional function for various conditional syntax errors

## Simplified Source
```c
static void ConditionError(const char *desc, int cmdn, const char *msg) {
    // Report fatal conditional error with script context
    pg_fatal("condition error in script \"%s\" command %d: %s",
             desc, cmdn, msg);
}
```