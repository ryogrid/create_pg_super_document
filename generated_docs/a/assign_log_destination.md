# assign_log_destination

## Location
[src/backend/utils/error/elog.c:2294-2302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2294-L2302)

## Overview
A GUC assign hook function that applies the validated log destination settings by updating the global Log_destination variable.

## Definition
```c
void assign_log_destination(const char *newval, void *extra)
```

## Detailed Description
This function serves as the assignment hook for the `log_destination` GUC parameter. It receives the validated destination bitmask from the corresponding check hook (check_log_destination) and assigns it to the global `Log_destination` variable. This variable controls where PostgreSQL writes its log output, supporting combinations of stderr, CSV files, JSON files, syslog, and Windows event log.

## Parameters / Member Variables
- `newval`: The new string value for the log_destination parameter (not used in this implementation)
- `extra`: A void pointer containing the validated destination bitmask created by check_log_destination

## Dependencies
- Functions called/Symbols referenced:
  - Log_destination (global variable)
- Called from (representative examples):
  - GUC system (via function pointer in guc_hooks.h)

## Notes and Other Information
- This function assumes validation has already been performed by check_log_destination
- The actual destination bitmask is passed through the extra parameter
- Changes to Log_destination take effect immediately for subsequent log messages
- Part of PostgreSQL's flexible logging system that supports multiple simultaneous destinations
- Located in src/backend/utils/error/elog.c:2294-2302

## Simplified Source

```c
void assign_log_destination(const char *newval, void *extra) {
    // Apply the validated log destination bitmask to global variable
    Log_destination = *((int *) extra);
}
```