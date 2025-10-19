# check_huge_page_size

## Location
[src/backend/port/sysv_shmem.c:578-598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/sysv_shmem.c#L578-L598)

## Overview
A GUC validation hook function that validates the  configuration parameter, ensuring it's only set to non-zero values on supported platforms.

## Definition

```c
bool
check_huge_page_size(int *newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a validation check hook for PostgreSQL's GUC (Grand Unified Configuration) system, specifically for the  parameter. It enforces platform-specific constraints by only allowing non-zero values on recent Linux systems that support MAP_HUGE_MASK and MAP_HUGE_SHIFT. On unsupported platforms, it rejects any attempt to set a non-zero huge page size and provides an appropriate error message.

The function is part of PostgreSQL's configuration validation framework, ensuring that users cannot set invalid huge page configurations that would cause runtime failures.

## Parameters / Member Variables
- `*newval`: Pointer to the new integer value being set for huge_page_size
- `**extra`: Pointer to extra data (unused in this implementation)
- `source`: The source of the configuration change (e.g., postgresql.conf, SET command)
## Dependencies
- Functions called/Symbols referenced:
  - GucSource
  - GUC_check_errdetail
- Called from (representative examples):
  - GUC_HOOKS_H (referenced in header declarations)

## Notes and Other Information
- Only permits non-zero huge_page_size values on platforms with MAP_HUGE_MASK and MAP_HUGE_SHIFT support
- Returns false with error detail when validation fails, preventing invalid configuration
- Part of PostgreSQL's GUC validation framework for configuration parameter checking
- Works in conjunction with GetHugePageSize() which actually implements the huge page functionality
- The validation is compile-time based on preprocessor definitions rather than runtime platform detection

## Simplified Source

```c
bool check_huge_page_size(int *newval, void **extra, GucSource source) {
#if !(defined(MAP_HUGE_MASK) && defined(MAP_HUGE_SHIFT))
    // Platform doesn't support huge pages - reject non-zero values
    if (*newval != 0) {
        GUC_check_errdetail("\"huge_page_size\" must be 0 on this platform.");
        return false;
    }
#endif
    return true;  // Value is valid
}
```