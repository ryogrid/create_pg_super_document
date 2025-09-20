# pg_attribute_noreturn

## Location
[src/backend/replication/logical/tablesync.c:143-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L143-L183)

## Overview
A preprocessor macro that applies the noreturn attribute to functions, indicating that the function never returns control to its caller.

## Definition
```c
#define pg_attribute_noreturn() __attribute__((noreturn))  // GCC/Sunpro
#define pg_attribute_noreturn()                           // MSVC/others (empty)
```

## Detailed Description
The `pg_attribute_noreturn` macro is a portable way to declare that a function never returns to its caller. On GCC and Sunpro compilers, it expands to `__attribute__((noreturn))`, which enables compiler optimizations and warnings. On MSVC and other compilers, it expands to nothing, maintaining compatibility.

Functions marked with this attribute typically:
- Call `exit()` or similar termination functions
- Throw exceptions or trigger error handlers
- Enter infinite loops
- Call other noreturn functions

The compiler uses this information to:
- Optimize code paths after noreturn function calls
- Issue warnings about unreachable code
- Avoid false warnings about uninitialized variables

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - `__attribute__((noreturn))` (GCC builtin)
- Called from (representative examples):
  - `error_multiple_recovery_targets` (src/backend/access/transam/xlogrecovery.c:4769)
  - `finish_sync_worker` (src/backend/replication/logical/tablesync.c:143)
  - [WalSndShutdown](../W/WalSndShutdown.md) (src/backend/replication/walsender.c:244)
  - [pg_fatal](pg_fatal.md) (src/bin/pg_upgrade/pg_upgrade.h:474)
  - Various error handling and exit functions throughout PostgreSQL

## Notes and Other Information
- Defined in `src/include/c.h` with conditional compilation based on compiler support
- Essential for proper compiler optimization in error handling paths
- MSVC version is empty because MSVC requires the attribute before function declaration, not after
- Part of PostgreSQL's portable attribute system for cross-compiler compatibility
- Helps prevent warnings about missing return statements in functions that never return