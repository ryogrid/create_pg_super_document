# get_stack_depth_rlimit

## Location
[src/backend/tcop/postgres.c:5048-5079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L5048-L5079)

## Overview
This function obtains the platform-specific stack depth limit in bytes, providing a portable way to determine the maximum stack size available to the PostgreSQL backend process.

## Definition
```c
long get_stack_depth_rlimit(void)
```

## Detailed Description
`get_stack_depth_rlimit` retrieves the system's stack size limit and caches the result for subsequent calls. The function handles platform differences by using POSIX `getrlimit` system call on Unix-like systems and a predefined constant on Windows. The implementation includes overflow protection and handles the special case of unlimited stack size by returning `LONG_MAX`. The result is cached since stack limits don't change during process execution, making subsequent calls very efficient.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - getrlimit (POSIX system call)
  - HAVE_GETRLIMIT (preprocessor macro)
  - WIN32_STACK_RLIMIT (Windows-specific constant)
  - RLIMIT_STACK (resource limit constant)
  - RLIM_INFINITY (unlimited resource constant)
  - LONG_MAX (maximum long value)

- Called from (representative examples):
  - check_max_stack_depth
  - InitializeGUCOptionsFromEnvironment

## Notes and Other Information
- Returns -1 if the stack limit cannot be determined
- Uses static caching to avoid repeated system calls
- Handles overflow protection when converting from potentially unsigned rlim_cur to signed long
- On Windows, uses a compile-time constant instead of runtime detection
- Critical for PostgreSQL's stack depth checking mechanism to prevent stack overflow crashes