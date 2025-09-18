# check_autovacuum_work_mem

## Location
src/backend/postmaster/autovacuum.c: 3364 - 3384

## Overview
check_autovacuum_work_mem is a GUC (Grand Unified Configuration) validation function that validates and adjusts the autovacuum_work_mem configuration parameter to ensure it meets minimum requirements.

## Definition


## Detailed Description
This function serves as a configuration validation hook for the autovacuum_work_mem PostgreSQL configuration parameter. It is automatically called by the GUC system whenever the autovacuum_work_mem parameter is being set or changed.

The function performs two key validation and adjustment operations:

1. **Fallback Value Handling**: If the value is -1, it allows this special value to pass through unchanged. A value of -1 indicates that autovacuum should fall back to using the maintenance_work_mem setting instead of having its own dedicated memory limit.

2. **Minimum Value Enforcement**: For any positive value, it enforces a minimum threshold of 64 kilobytes. This ensures that autovacuum workers have sufficient memory to perform their operations effectively. Values below 64kB are automatically adjusted upward to 64kB.

The 64kB minimum is consistent with the minimum value enforced for maintenance_work_mem, ensuring consistency between related memory configuration parameters.

## Parameters / Member Variables
- : Pointer to the new integer value being set (in kilobytes). This value may be modified by the function to enforce constraints.
- : Pointer to extra data (unused in this function, can be NULL)
- : The source of the configuration change (e.g., configuration file, SQL command, etc.)

**Return value**: Always returns true, indicating the value is acceptable (after any necessary adjustments)

## Dependencies
- Functions called/Symbols referenced:
  - GucSource (enumeration type for configuration sources)
- Called from:
  - PostgreSQL GUC system (as a check hook)
- Referenced in:
  - src/include/utils/guc_hooks.h:34 (function declaration)

## Notes and Other Information
- This is a GUC check hook function, part of PostgreSQL's configuration system
- The function always returns true because it adjusts invalid values rather than rejecting them
- The -1 special value allows autovacuum to inherit the maintenance_work_mem setting
- The 64kB minimum ensures adequate memory for autovacuum operations
- Memory values in PostgreSQL GUC are typically specified in kilobytes
- This function is automatically called by the GUC system; users don't call it directly
- Located in src/backend/postmaster/autovacuum.c:3364-3384