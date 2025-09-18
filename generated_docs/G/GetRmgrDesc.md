# GetRmgrDesc

## Location
src/bin/pg_waldump/rmgrdesc.c: 87 - 99

## Overview
Retrieves the resource manager descriptor for a given resource manager ID, handling both built-in and custom resource managers.

## Definition
```c
const RmgrDescData *GetRmgrDesc(RmgrId rmid)
```

## Detailed Description
The `GetRmgrDesc` function serves as the main interface for obtaining resource manager descriptors in pg_waldump. It takes a resource manager ID and returns a pointer to the corresponding RmgrDescData structure. The function handles two categories of resource managers: built-in resource managers (which have pre-defined descriptors in RmgrDescTable) and custom resource managers (which require lazy initialization through initialize_custom_rmgrs). This design allows pg_waldump to handle WAL records from both standard PostgreSQL resource managers and extension-provided custom resource managers.

## Parameters / Member Variables
- `rmid`: RmgrId value identifying which resource manager descriptor to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - RmgrId (typedef for resource manager identifier)
  - RmgrIdIsValid (macro/function to validate resource manager ID)
  - [RmgrIdIsBuiltin](../R/RmgrIdIsBuiltin.md) (macro/function to check if ID is for built-in resource manager)
  - [initialize_custom_rmgrs](../i/initialize_custom_rmgrs.md) (called for lazy initialization of custom resource managers)
  - RM_MIN_CUSTOM_ID (constant defining minimum custom resource manager ID)
- Called from (representative examples):
  - [print_rmgr_list](../p/print_rmgr_list.md) (in pg_waldump.c to list available resource managers)
  - [XLogDumpDisplayRecord](../X/XLogDumpDisplayRecord.md) (in pg_waldump.c to display WAL record information)
  - [XLogDumpDisplayStats](../X/XLogDumpDisplayStats.md) (in pg_waldump.c to display statistics)
  - [main](../m/main.md) (in pg_waldump.c as part of command-line processing)

## Notes and Other Information
- Function includes assertion to ensure the resource manager ID is valid
- Uses lazy initialization for custom resource managers to avoid unnecessary setup
- Returns a const pointer to prevent modification of resource manager descriptors
- Critical function in pg_waldump's resource manager handling system
- Bridges the gap between built-in and custom resource manager handling
- The returned descriptor contains name, description function, and identification function pointers