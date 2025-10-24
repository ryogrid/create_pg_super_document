# assign_search_path

## Location
[src/backend/catalog/namespace.c:4713-4735](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4713-L4735)

## Overview
A GUC assign hook function that handles the assignment of a new value to the search_path configuration parameter by marking the current path as needing recomputation.

## Definition

```c
void
assign_search_path(const char *newval, void *extra)
```
## Detailed Description
This function serves as the assign hook for the search_path GUC (Grand Unified Configuration) parameter. It is called whenever the search_path configuration is updated through SET commands or other configuration mechanisms. Rather than immediately recomputing the search path, it adopts a lazy evaluation strategy by simply marking the current baseSearchPath as invalid, deferring the actual recomputation until the search path is next accessed. This design avoids expensive database operations during GUC initialization or when outside a transaction context.

## Parameters / Member Variables
- `*newval`: The new string value being assigned to the search_path parameter
- `*extra`: Additional data passed by the GUC system (currently unused in this function)
## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- This function must not be called during bootstrap processing mode, enforced by an Assert
- The function uses lazy evaluation to avoid database access during inappropriate times
- Does not invalidate the search path cache, allowing for optimization when no syscache invalidations have occurred
- Part of the PostgreSQL GUC (Grand Unified Configuration) system hook mechanism

## Simplified Source

```c
void
assign_search_path(const char *newval, void *extra)
{
    // Ensure we're not in bootstrap mode
    Assert(!IsBootstrapProcessingMode());

    // Mark search path as needing recomputation (lazy evaluation)
    // Actual recomputation is deferred until the path is accessed
    baseSearchPathValid = false;
}
```