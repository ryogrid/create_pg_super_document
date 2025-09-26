# check_log_destination

## Location
[src/backend/utils/error/elog.c:2232-2293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2232-L2293)

## Overview
A GUC check hook function that validates and parses the log_destination configuration parameter, ensuring that specified logging destinations are valid and supported.

## Definition
```c
bool check_log_destination(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function validates the `log_destination` configuration parameter by parsing a comma-separated list of logging destinations. It supports multiple output formats including stderr, csvlog, jsonlog, syslog (if available), and eventlog (on Windows). The function splits the input string, validates each destination against known values, and builds a bitmask representing the selected destinations. If validation succeeds, it allocates memory to store the bitmask for use by the corresponding assign hook.

## Parameters / Member Variables
- `newval`: Pointer to the string value being validated for the log_destination parameter
- `extra`: Pointer to store additional data (the destination bitmask) for the assign hook
- `source`: The source of the configuration change (not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md)
  - [SplitIdentifierString](../S/SplitIdentifierString.md)
  - GUC_check_errdetail
  - [list_free](../l/list_free.md)
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - LOG_DESTINATION_STDERR
  - LOG_DESTINATION_CSVLOG
  - LOG_DESTINATION_JSONLOG
  - LOG_DESTINATION_SYSLOG
  - LOG_DESTINATION_EVENTLOG
  - [guc_malloc](../g/guc_malloc.md)
- Called from (representative examples):
  - GUC system (via function pointer in guc_hooks.h)

## Notes and Other Information
- Supports conditional compilation for syslog (HAVE_SYSLOG) and Windows eventlog (WIN32)
- Uses case-insensitive string comparison for destination names
- Properly handles memory management by freeing temporary allocations on error
- Returns allocated memory via the extra parameter for use by assign_log_destination
- Located in src/backend/utils/error/elog.c:2232-2293