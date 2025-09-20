# InjectionPointRun

## Location
[src/backend/utils/misc/injection_point.c:526-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/injection_point.c#L526-L537)

## Overview
Executes an injection point by name if it exists, providing the primary interface for triggering dynamic code injection during PostgreSQL execution.

## Definition

```c
void
InjectionPointRun(const char *name)
```
## Detailed Description
This function serves as the main entry point for executing injection points in PostgreSQL's testing infrastructure. It provides a simple interface that hides the complexity of cache management and shared memory synchronization. The function:

1. **Cache Lookup**: Uses InjectionPointCacheRefresh to find the injection point in the local cache or shared memory
2. **Execution**: If found, calls the associated callback function with the injection point name and private data
3. **No-op Behavior**: If the injection point doesn't exist, the function simply returns without error

The function is designed to be lightweight and fast, suitable for embedding throughout the PostgreSQL codebase without significant performance impact when injection points are not active.

## Parameters / Member Variables
- : The unique identifier of the injection point to execute

## Dependencies
- Functions called/Symbols referenced:
  - [InjectionPointCacheRefresh](InjectionPointCacheRefresh.md)
  - [callback](../c/callback.md) (function pointer from cached entry)
- Types referenced:
  - [InjectionPointCacheEntry](InjectionPointCacheEntry.md)
- Called from:
  - INJECTION_POINT macro (src/include/utils/injection_point.h:18)

## Notes and Other Information
- Only functional when compiled with USE_INJECTION_POINTS defined
- This is the primary public interface for injection point execution
- Typically called through the INJECTION_POINT() macro rather than directly
- The function performs no error checking - non-existent injection points are silently ignored
- Callback functions receive both the injection point name and any associated private data
- Used throughout PostgreSQL source code via the INJECTION_POINT() macro for testing and debugging
- The lightweight design allows injection points to be placed in performance-critical code paths
- When USE_INJECTION_POINTS is not defined, the function throws an error if called