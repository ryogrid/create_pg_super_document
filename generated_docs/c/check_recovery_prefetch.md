# check_recovery_prefetch

## Location
[src/backend/access/transam/xlogprefetcher.c:1083-1096](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L1083-L1096)

## Overview
check_recovery_prefetch is a GUC (Grand Unified Configuration) validation hook function that validates the recovery_prefetch configuration parameter, ensuring it's only enabled on platforms that support the required prefetching functionality.

## Definition

```c
bool
check_recovery_prefetch(int *new_value, void **extra, GucSource source)
```
## Detailed Description
This function serves as a validation hook for the  GUC parameter in PostgreSQL's configuration system. It performs platform-specific validation to ensure that recovery prefetching is only enabled on systems that have the necessary infrastructure support.

The primary validation is checking if the system supports , which is required for the prefetching mechanism. On platforms that lack this functionality (identified by the absence of the  compile-time macro), the function prevents enabling recovery prefetching and provides a descriptive error message.

This validation occurs whenever the  setting is modified, whether through configuration files, SQL commands, or other GUC modification mechanisms.

## Parameters / Member Variables
- `*new_value`: Pointer to the integer value representing the new setting for recovery_prefetch
- `**extra`: Pointer to additional data (unused in this function, but part of the GUC hook interface)
- `source`: GucSource enumeration indicating how the configuration change was initiated (e.g., configuration file, SQL command, etc.)
## Dependencies
- Functions called/Symbols referenced:
  -  - Sets detailed error message for GUC validation failures
  -  - Compile-time macro indicating platform support for prefetching
  -  - Constant representing the "on" state for recovery prefetching
  -  - Enumeration type for GUC parameter sources
- Called from (representative examples):
  - GUC system in src/backend/utils/misc/guc_tables.c:5053 as part of recovery_prefetch parameter definition

## Notes and Other Information
- This is a standard GUC validation hook that follows PostgreSQL's configuration parameter validation pattern
- The function returns  for valid configurations and  for invalid ones
- On platforms without  support, only non-ON values are acceptable
- The function is registered in the GUC system alongside  as part of the recovery_prefetch parameter configuration
- Platform compatibility is determined at compile time, making this a static validation
- The error message specifically mentions the lack of  support to help administrators understand the limitation

## Simplified Source

```c
bool check_recovery_prefetch(int *new_value, void **extra, GucSource source) {
#ifndef USE_PREFETCH
    // Reject ON setting if platform lacks posix_fadvise() support
    if (*new_value == RECOVERY_PREFETCH_ON) {
        GUC_check_errdetail("recovery_prefetch not supported without posix_fadvise()");
        return false;
    }
#endif

    return true;  // Valid configuration
}
```