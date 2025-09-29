# SyncRepGetStandbyPriority

## Location
[src/backend/replication/syncrep.c:860-906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L860-L906)

## Overview
Determines the synchronous standby priority for the current walsender by checking if its application_name matches any entry in the configured list of synchronous standbys.

## Definition
```c
static int SyncRepGetStandbyPriority(void)
```

## Detailed Description
This function evaluates whether the current walsender process is eligible to be a synchronous standby and, if so, determines its priority level. The function performs several checks:

1. Cascading walsenders are automatically excluded (priority 0) since synchronous cascade replication is not allowed
2. If synchronous standbys are not defined or configured, returns 0
3. Iterates through the configured standby names to find a match with the current application_name
4. Supports wildcard matching using "*" to include any standby name
5. In priority mode, returns the position-based priority (1-based index)
6. In quorum mode, all matched standbys receive priority 1

The function uses case-insensitive comparison for standby names and supports exact name matching or wildcard inclusion.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - SyncStandbysDefined (checks if sync standbys are configured)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (case-insensitive string comparison)
  - strcmp (string comparison for wildcard check)
  - strlen (string length calculation)
- Called from (representative examples):
  - SyncStandbysDefined (src/backend/replication/syncrep.c:119)
  - [SyncRepInitConfig](SyncRepInitConfig.md) (src/backend/replication/syncrep.c:453)

## Notes and Other Information
- Returns 0 for non-sync standbys or when sync replication is disabled
- Returns priority value (1-based) for matched standbys in priority mode
- Returns 1 for all matched standbys in quorum mode
- Cascading walsenders are explicitly excluded from synchronous replication
- Uses global variables: am_cascading_walsender, SyncRepConfig, application_name
- Static function scope limits visibility to the syncrep.c compilation unit

## Simplified Source

```c
// Simplified version of SyncRepGetStandbyPriority
static int SyncRepGetStandbyPriority(void) {
    const char *standby_name;
    int priority;
    bool found = false;

    // Cascading walsenders cannot be sync standbys
    if (am_cascading_walsender)
        return 0;

    // Check if sync replication is configured
    if (!SyncStandbysDefined() || SyncRepConfig == NULL)
        return 0;

    // Search through configured standby names
    standby_name = SyncRepConfig->member_names;
    for (priority = 1; priority <= SyncRepConfig->nmembers; priority++) {
        // Match application name or wildcard "*"
        if (pg_strcasecmp(standby_name, application_name) == 0 ||
            strcmp(standby_name, "*") == 0) {
            found = true;
            break;
        }
        // Move to next name in the list
        standby_name += strlen(standby_name) + 1;
    }

    if (!found)
        return 0;

    // Return priority: position-based for PRIORITY mode, 1 for QUORUM mode
    return (SyncRepConfig->syncrep_method == SYNC_REP_PRIORITY) ? priority : 1;
}
```

Key simplifications made:
- Condensed comments to focus on essential logic flow
- Preserved the exact algorithm and control flow
- Maintained all critical checks and conditions
- Simplified variable declarations for clarity
- Added inline comments explaining each major step
- Kept the original return logic for both priority and quorum modes