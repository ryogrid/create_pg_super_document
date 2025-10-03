# assign_synchronous_standby_names

## Location
[src/backend/replication/syncrep.c:1115-1120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L1115-L1120)

## Overview
A GUC assign hook function that applies the validated synchronous_standby_names configuration after it has been successfully checked and parsed.

## Definition

```c
void
assign_synchronous_standby_names(const char *newval, void *extra)
```
## Detailed Description
This function serves as the assignment hook for the synchronous_standby_names GUC parameter in PostgreSQL's configuration system. It is called after the check_synchronous_standby_names function has successfully validated a new configuration value. The function's primary responsibility is to update the global SyncRepConfig variable with the parsed configuration data that was prepared during the validation phase.

This is a simple but critical function in the GUC lifecycle - it takes the validated and parsed configuration data (stored in the extra parameter) and makes it the active configuration for synchronous replication behavior throughout the PostgreSQL system.

## Parameters / Member Variables
- `*newval`: The new string value for synchronous_standby_names (not directly used in this function)
- `*extra`: Pointer to the validated SyncRepConfigData structure created during the check phase
## Dependencies
- Functions called/Symbols referenced:
  - [SyncRepConfigData](../S/SyncRepConfigData.md) (cast to this type)
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- This function assumes that the extra parameter contains valid parsed configuration data from check_synchronous_standby_names
- Updates the global SyncRepConfig variable which is used throughout the synchronous replication system
- The function does not perform any validation - it relies on the prior check hook to ensure data validity
- Part of the standard GUC three-phase protocol: check, assign, and show hooks
- The extra parameter is expected to be allocated with guc_malloc and will be managed by the GUC system
- This assignment makes the new synchronous replication configuration immediately active for all subsequent replication operations