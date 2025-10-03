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

## Dependencies
- Functions called/Symbols referenced:
  - getrlimit (POSIX system call)
  - HAVE_GETRLIMIT (preprocessor macro)
  - WIN32_STACK_RLIMIT (Windows-specific constant)
  - RLIMIT_STACK (resource limit constant)
  - RLIM_INFINITY (unlimited resource constant)
  - LONG_MAX (maximum long value)

- Called from (representative examples):
  - [check_max_stack_depth](../c/check_max_stack_depth.md)
  - [InitializeGUCOptionsFromEnvironment](../I/InitializeGUCOptionsFromEnvironment.md)

## Notes and Other Information
- Returns -1 if the stack limit cannot be determined
- Uses static caching to avoid repeated system calls
- Handles overflow protection when converting from potentially unsigned rlim_cur to signed long
- On Windows, uses a compile-time constant instead of runtime detection
- Critical for PostgreSQL's stack depth checking mechanism to prevent stack overflow crashes

## Simplified Source

```c
// Simplified version of get_stack_depth_rlimit
long get_stack_depth_rlimit(void) {
    // Cache the result since stack limits don't change during process execution
    static long cached_limit = 0;

    if (cached_limit == 0) {
#if defined(HAVE_GETRLIMIT)
        // Unix/Linux systems: use POSIX getrlimit system call
        struct rlimit stack_limit;

        if (getrlimit(RLIMIT_STACK, &stack_limit) < 0) {
            // System call failed - unknown limit
            cached_limit = -1;
        } else if (stack_limit.rlim_cur == RLIM_INFINITY) {
            // Unlimited stack - return maximum possible value
            cached_limit = LONG_MAX;
        } else if (stack_limit.rlim_cur >= LONG_MAX) {
            // Prevent overflow when converting to signed long
            cached_limit = LONG_MAX;
        } else {
            // Normal case - return the actual limit
            cached_limit = stack_limit.rlim_cur;
        }
#else
        // Windows systems: use compile-time constant
        cached_limit = WIN32_STACK_RLIMIT;
#endif
    }

    return cached_limit;
}
```

Key simplifications made:
- Renamed variable `val` to `cached_limit` for clarity
- Renamed `rlim` to `stack_limit` for better readability
- Added descriptive comments for each major logic branch
- Consolidated the overflow checking logic with clearer explanations
- Simplified the conditional structure while preserving all functionality
- Emphasized the caching mechanism and platform differences