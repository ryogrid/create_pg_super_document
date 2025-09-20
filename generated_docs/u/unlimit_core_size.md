# unlimit_core_size

## Location
[src/bin/pg_ctl/pg_ctl.c:774-793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L774-L793)

## Overview
Increases the core file size limit to its maximum allowed value to enable full core dump generation for debugging purposes.

## Definition

```c
struct rlimit lim;
```
## Detailed Description
The  function modifies the process resource limits to maximize the size of core files that can be generated when the process crashes. This is essential for debugging PostgreSQL issues, as truncated core files may not contain enough information for effective analysis.

**Key behaviors:**
- Retrieves current core file size limits using 
- Checks if the hard limit allows core file generation (hard limit > 0)
- Sets the soft limit (current limit) to match the hard limit (maximum allowed)
- Handles cases where core files are completely disabled by the system
- Provides error messaging when core file limits cannot be modified

**Limit handling logic:**
1. If hard limit is 0: Core files are completely disabled by system policy - reports error and returns
2. If hard limit is RLIM_INFINITY or soft limit is less than hard limit: Sets soft limit to hard limit
3. Otherwise: No action needed as limits are already optimal

This ensures that PostgreSQL processes can generate complete core dumps when they crash, which is crucial for post-mortem debugging.

## Parameters / Member Variables
This function takes no parameters and uses local variables:
- : struct rlimit containing current (rlim_cur) and maximum (rlim_max) core file size limits

## Dependencies
- Functions called/Symbols referenced:
  -  - Retrieves current resource limits
  -  - Sets new resource limits  
  -  - Reports error messages
- Called from (representative examples):
  -  in pg_ctl.c
  -  in pg_regress.c

## Notes and Other Information
- Only available on systems with  support (Unix-like systems)
- Core file generation is often disabled by default on many systems for security reasons
- Essential for PostgreSQL development and production debugging workflows
- The function is silent on success, only reporting when core files cannot be enabled
- Typically called early in process startup to ensure debugging capabilities are available
- Used by both pg_ctl (for postmaster startup) and regression tests (for debugging test failures)