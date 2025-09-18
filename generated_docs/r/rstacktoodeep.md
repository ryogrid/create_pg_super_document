# rstacktoodeep

## Location
[src/backend/regex/regcomp.c:2483-2493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2483-L2493)

## Overview
Checks if the function call stack has grown dangerously deep during regex compilation, providing a safety mechanism to prevent stack overflow errors.

## Definition


## Detailed Description
The rstacktoodeep function serves as a critical safety mechanism in PostgreSQL's regex compilation engine to prevent stack overflow conditions. During complex regex compilation operations, particularly with deeply nested patterns or recursive structures, the call stack can grow to dangerous levels that might exceed system limits and cause crashes.

This function acts as a sentinel that monitors stack depth and provides an early warning system. When called during regex compilation operations, it checks the current stack usage against safe thresholds. If the stack is determined to be too deep, it returns a nonzero value, which typically causes the calling regex compilation function to abort with a REG_ETOOBIG error code.

The current implementation is PostgreSQL-specific and leverages the database's existing stack depth monitoring infrastructure (stack_is_too_deep()). This ensures consistency with PostgreSQL's overall approach to preventing stack overflow in recursive operations across the entire database system.

The function is designed to be called at strategic points during regex compilation where deep recursion is possible, such as during parse tree processing or NFA construction. It provides a clean way to fail gracefully rather than experiencing hard stack overflow crashes.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - stack_is_too_deep - PostgreSQL's core stack depth checking function
- Called from (representative examples):
  - COLORED - Color processing operations that may recurse deeply

## Notes and Other Information
- Returns nonzero to indicate stack is too deep (should fail with REG_ETOOBIG)
- Returns zero to indicate safe to continue operation
- PostgreSQL-specific implementation using existing database stack monitoring
- Designed for potential future extraction as standalone library with callback API
- Critical for preventing stack overflow crashes in complex regex patterns
- Part of PostgreSQL's defensive programming approach to resource limits
- Typically causes compilation to fail with REG_ETOOBIG error when triggered
- Strategic placement in recursive compilation paths provides safety net