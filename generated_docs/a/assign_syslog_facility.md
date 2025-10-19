# assign_syslog_facility

## Location
[src/backend/utils/error/elog.c:2335-2359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2335-L2359)

## Overview
A GUC assign hook function that updates the syslog facility value and manages syslog connection state when the syslog_facility parameter changes.

## Definition
```c
void assign_syslog_facility(int newval, void *extra)
```

## Detailed Description
This function manages the syslog facility setting used by PostgreSQL when logging to syslog. Similar to assign_syslog_ident, it intelligently manages the syslog connection by only closing and reopening it when the facility value actually changes. The facility determines which syslog category the messages are logged under (e.g., LOG_LOCAL0, LOG_LOCAL1, etc.). The function avoids unnecessary connection cycling by comparing the current facility with the new value.

## Parameters / Member Variables
- `newval`: The new integer syslog facility value to be set
- `extra`: Additional data from the GUC system (not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - closelog
  - syslog_facility (global variable)
  - openlog_done (global variable)
- Called from (representative examples):
  - GUC system (via function pointer in guc_hooks.h)

## Notes and Other Information
- Only compiled and functional when HAVE_SYSLOG is defined
- Avoids unnecessary syslog connection cycling by comparing old and new facility values
- The syslog connection is reopened lazily when needed rather than immediately
- Works in conjunction with assign_syslog_ident to manage syslog configuration
- The facility value corresponds to standard syslog facilities defined in syslog.h
- Located in src/backend/utils/error/elog.c:2335-2359

## Simplified Source

```c
void assign_syslog_facility(int newval, void *extra) {
#ifdef HAVE_SYSLOG
    // Only update if the facility actually changed
    if (syslog_facility != newval) {
        // Close existing syslog connection if open
        if (openlog_done) {
            closelog();
            openlog_done = false;
        }

        // Update to new facility value
        syslog_facility = newval;
    }
#endif
    // Without syslog support, do nothing
}
```