# check_primary_slot_name

## Location
[src/backend/access/transam/xlogrecovery.c:4741-4768](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4741-L4768)

## Overview
check_primary_slot_name is a GUC (Grand Unified Configuration) check hook function that validates the primary_slot_name configuration parameter to ensure it contains a valid replication slot name.

## Definition
```c
bool check_primary_slot_name(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook for the primary_slot_name GUC parameter in PostgreSQL's configuration system. It is called whenever the primary_slot_name parameter is being set or changed, ensuring that only valid replication slot names are accepted. The function validates the slot name using PostgreSQL's built-in replication slot name validation rules, which typically include restrictions on length, character set, and format.

The validation allows empty strings (which effectively disable the primary slot feature) but rejects invalid slot names, providing early feedback when invalid configurations are attempted.

## Parameters / Member Variables
- `newval`: Double pointer to the new string value being assigned to primary_slot_name (can be modified by the hook)
- `extra`: Double pointer for storing additional data that may be used by assign hooks (unused in this function)
- `source`: Indicates the source of the configuration change (e.g., config file, command line, SQL SET command)

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotValidateName](../R/ReplicationSlotValidateName.md)
  - GucSource (enum type)
- Called from:
  - PostgreSQL GUC system (registered as check hook in guc_hooks.h)

## Notes and Other Information
- Returns true if the validation passes, false if the new value should be rejected
- Empty strings ("") are explicitly allowed and pass validation
- Uses WARNING level for validation error reporting through ReplicationSlotValidateName
- This is part of PostgreSQL's GUC system architecture where check hooks provide parameter validation
- The primary_slot_name parameter is used in streaming replication to specify which slot on the primary server the standby should use
- Validation occurs before the value is actually assigned, preventing invalid configurations from being stored