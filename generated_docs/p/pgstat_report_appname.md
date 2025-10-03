# pgstat_report_appname

## Location
[src/backend/utils/activity/backend_status.c:653-681](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L653-L681)

## Overview
Updates the application name field in the backend status entry, enabling identification of different client applications in PostgreSQL's monitoring views.

## Definition

```c
void
pgstat_report_appname(const char *appname)
```
## Detailed Description
This function updates the application name stored in the backend's status entry in shared memory. The application name is typically set by client applications through the application_name connection parameter or GUC setting, allowing database administrators to identify and monitor different applications or components connecting to the database.

The function performs proper multi-byte character handling by clipping the application name to fit within the allocated storage space (NAMEDATALEN - 1 characters). It uses the standard change-counting protocol to ensure atomic updates to the shared status structure, preventing readers from seeing partial or inconsistent data during the update process.

## Parameters / Member Variables
- `*appname`: The application name string to be stored in the backend status (null-terminated C string)
## Dependencies
- Functions called/Symbols referenced:
  - [pg_mbcliplen](pg_mbcliplen.md)
  - PGSTAT_BEGIN_WRITE_ACTIVITY
  - PGSTAT_END_WRITE_ACTIVITY
- Called from (representative examples):
  - [assign_application_name](../a/assign_application_name.md)
  - [pgstat_bestart](pgstat_bestart.md)

## Notes and Other Information
- Application names are visible in monitoring views like pg_stat_activity
- The function safely handles multi-byte character sets by clipping at character boundaries
- Application name length is limited to NAMEDATALEN - 1 characters (typically 63 characters)
- Uses volatile pointers and atomic update protocol to ensure thread safety
- Typically called when the application_name GUC parameter is set or changed
- Part of PostgreSQL's client connection tracking and monitoring infrastructure
- Returns early if no backend status entry exists (MyBEEntry is NULL)

## Simplified Source

```c
// Simplified version of pgstat_report_appname
void pgstat_report_appname(const char *appname) {
    volatile PgBackendStatus *status_entry = MyBEEntry;

    // Return early if no backend status entry
    if (!status_entry) {
        return;
    }

    // Clip application name to fit within allocated space
    int safe_length = pg_mbcliplen(appname, strlen(appname), NAMEDATALEN - 1);

    // Atomic update of status entry with change counting protocol
    PGSTAT_BEGIN_WRITE_ACTIVITY(status_entry);

    // Copy application name and null terminate
    memcpy((char *) status_entry->st_appname, appname, safe_length);
    status_entry->st_appname[safe_length] = '\0';

    PGSTAT_END_WRITE_ACTIVITY(status_entry);
}
```

Key simplifications made:
- Added descriptive variable names and inline comments
- Clarified the purpose of each major operation
- Core logic: Check status entry exists, clip name length, atomic update with protocol