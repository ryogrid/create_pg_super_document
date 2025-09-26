# check_temp_buffers

## Location
[src/backend/storage/buffer/localbuf.c:704-727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L704-L727)

## Overview
check_temp_buffers is a GUC (Grand Unified Configuration) check hook function that validates attempts to change the temp_buffers configuration parameter.

## Definition
```c
bool check_temp_buffers(int *newval, void **extra, GucSource source)
```

## Detailed Description
check_temp_buffers serves as a validation function for the temp_buffers GUC parameter, which controls the maximum number of temporary buffers used by each database session. The function implements a critical restriction: once local buffers have been initialized in a session (indicated by NLocBuffer being non-zero), the temp_buffers setting cannot be changed.

This restriction exists because PostgreSQL allocates and initializes the local buffer pool early in the session, and changing the size after initialization would require complex reallocation logic that could disrupt ongoing operations. The function allows changes only during testing scenarios (when source is PGC_S_TEST) or when no local buffers have been allocated yet.

When an invalid change attempt is detected, the function uses GUC_check_errdetail to provide a clear error message explaining why the change cannot be made.

## Parameters / Member Variables
- `newval`: Pointer to the new value being proposed for temp_buffers
- `extra`: Pointer to additional data (not used in this function)
- `source`: The source of the configuration change (test, config file, command line, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errdetail (for error reporting)
- Constants/Types referenced:
  - GucSource (enum type)
  - PGC_S_TEST (test source constant)
- Global variables accessed:
  - NLocBuffer (number of local buffers currently allocated)
- Called from (representative examples):
  - GUC system infrastructure (referenced in guc_hooks.h)

## Notes and Other Information
- This function is part of PostgreSQL's GUC (configuration parameter) validation system
- The temp_buffers parameter controls the size of the local buffer pool used for temporary tables and other temporary objects
- Once temporary tables are accessed in a session, the local buffer pool is initialized and cannot be resized
- Test calls (PGC_S_TEST) are allowed to bypass the restriction for validation purposes
- The function follows the standard GUC check hook pattern, returning true for valid values and false for invalid ones