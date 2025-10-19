# assign_syslog_ident

## Location
[src/backend/utils/error/elog.c:2303-2334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2303-L2334)

## Overview
A GUC assign hook function that updates the syslog identifier string and manages syslog connection state when the syslog_ident parameter changes.

## Definition
```c
void assign_syslog_ident(const char *newval, void *extra)
```

## Detailed Description
This function manages the syslog identifier string used by PostgreSQL when logging to syslog. It intelligently handles connection management by only reopening the syslog connection when the identifier actually changes, avoiding unnecessary overhead from repeated calls with the same value. The function makes its own copy of the identifier string to ensure independence from the GUC system's string management, and properly closes any existing syslog connection before updating the identifier.

## Parameters / Member Variables
- `newval`: The new syslog identifier string to be set
- `extra`: Additional data from the GUC system (not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp
  - closelog
  - free
  - strdup
  - syslog_ident (global variable)
  - openlog_done (global variable)
- Called from (representative examples):
  - GUC system (via function pointer in guc_hooks.h)

## Notes and Other Information
- Only compiled and functional when HAVE_SYSLOG is defined
- Gracefully handles strdup failure - [write_syslog](../w/write_syslog.md)() will cope with NULL syslog_ident
- Avoids unnecessary syslog connection cycling by comparing old and new values
- The syslog connection is reopened lazily when needed rather than immediately
- Properly manages memory by freeing the old identifier before setting the new one
- Located in src/backend/utils/error/elog.c:2303-2334

## Simplified Source

```c
void assign_syslog_ident(const char *newval, void *extra) {
#ifdef HAVE_SYSLOG
    // Only update if the identifier actually changed
    if (syslog_ident == NULL || strcmp(syslog_ident, newval) != 0) {
        // Close existing syslog connection if open
        if (openlog_done) {
            closelog();
            openlog_done = false;
        }

        // Update to new identifier
        free(syslog_ident);
        syslog_ident = strdup(newval);
        // Note: strdup failure is handled gracefully by write_syslog()
    }
#endif
    // Without syslog support, do nothing
}
```