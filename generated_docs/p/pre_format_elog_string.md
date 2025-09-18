# pre_format_elog_string

## Location
[src/backend/utils/error/elog.c:1645-1653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1645-L1653)

## Overview
Saves the error number and text domain before formatting an error message, preserving these values before argument evaluation can modify them.

## Definition
```c
void pre_format_elog_string(int errnumber, const char *domain)
```

## Detailed Description
The `pre_format_elog_string` function is a preparatory function used in PostgreSQL's error logging system. Its primary purpose is to preserve the current errno value and the caller's text domain before the actual error message formatting begins. This is crucial because the evaluation of argument functions during message formatting could potentially change the errno value, leading to incorrect error reporting.

The function stores the provided error number and domain in global variables (`save_format_errnumber` and `save_format_domain`) that can be safely accessed during the subsequent formatting process. This ensures that the original error context is maintained throughout the error reporting process.

## Parameters / Member Variables
- `errnumber`: The system error number (errno) to be preserved
- `domain`: The text domain for message translation/localization

## Dependencies
- Functions called/Symbols referenced:
  - save_format_errnumber (global variable)
  - save_format_domain (global variable)
- Called from (representative examples):
  - arch_module_check_errdetail
  - ereturn
  - GUC_check_errmsg
  - GUC_check_errdetail  
  - GUC_check_errhint

## Notes and Other Information
- This function is part of the error message formatting infrastructure
- Critical for preserving errno values that might be changed during argument evaluation
- Works in conjunction with `format_elog_string` to complete the error formatting process
- Used in various PostgreSQL subsystems including GUC (Grand Unified Configuration) and archive modules
- The saved values are typically used by subsequent formatting functions