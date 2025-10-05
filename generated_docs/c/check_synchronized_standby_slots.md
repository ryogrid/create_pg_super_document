# check_synchronized_standby_slots

## Location
[src/backend/replication/slot.c:2488-2543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L2488-L2543)

## Overview
GUC check_hook for the synchronized_standby_slots configuration parameter that validates the slot names and prepares configuration data for use by the system.

## Definition

```c
struct */
	size = offsetof(SyncStandbySlotsConfigData, slot_names);
```
## Detailed Description
This function serves as a GUC (Grand Unified Configuration) check hook for the synchronized_standby_slots parameter. It validates the provided slot names using validate_sync_standby_slots, then transforms the parsed slot names into a SyncStandbySlotsConfigData structure that can be efficiently used at runtime. The function handles empty configuration strings, allocates memory using guc_malloc (required for GUC extra values), and stores the slot names in a contiguous memory layout for optimal access. If validation fails or memory allocation fails, appropriate cleanup is performed.

## Parameters / Member Variables
- `newval`: Pointer to the new GUC value string being set
- `extra`: Output parameter that receives the prepared SyncStandbySlotsConfigData structure
- `source`: The source of the GUC value change (command line, config file, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [validate_sync_standby_slots](../v/validate_sync_standby_slots.md) (for validating slot names)
  - [pstrdup](../p/pstrdup.md) (for creating modifiable copy)
  - [list_free](../l/list_free.md) (for cleaning up parsed list)
  - [guc_malloc](../g/guc_malloc.md) (for allocating GUC extra data)
  - [list_length](../l/list_length.md) (for getting slot count)
  - strcpy/strlen (for string operations)
  - [pfree](../p/pfree.md) (for memory cleanup)
  - foreach_ptr (macro for list iteration)
  - [SyncStandbySlotsConfigData](../S/SyncStandbySlotsConfigData.md) (configuration structure)
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- This function is called by PostgreSQL's GUC system when the synchronized_standby_slots parameter is being set or changed
- Uses guc_malloc instead of palloc because GUC extra values have different memory management requirements
- Transforms list of slot names into a packed structure for efficient runtime access
- Returns true for empty configuration strings (allowing the parameter to be cleared)
- Performs proper cleanup of temporary memory allocations on both success and failure paths
- The resulting SyncStandbySlotsConfigData structure contains both the count of slots and the slot names in a contiguous memory layout

## Simplified Source

```c
bool
check_synchronized_standby_slots(char **newval, void **extra, GucSource source)
{
    char *rawname;
    char *ptr;
    List *elemlist;
    int size;
    bool ok;
    SyncStandbySlotsConfigData *config;

    // Allow empty configuration
    if ((*newval)[0] == '\0')
        return true;

    // Create modifiable copy and validate
    rawname = pstrdup(*newval);
    ok = validate_sync_standby_slots(rawname, &elemlist);

    if (!ok || elemlist == NIL)
    {
        pfree(rawname);
        list_free(elemlist);
        return ok;
    }

    // Calculate size needed for config structure
    size = offsetof(SyncStandbySlotsConfigData, slot_names);
    foreach_ptr(char, slot_name, elemlist)
        size += strlen(slot_name) + 1;

    // Allocate config structure (must use guc_malloc)
    config = (SyncStandbySlotsConfigData *) guc_malloc(LOG, size);
    if (!config)
        return false;

    // Pack slot names into structure
    config->nslotnames = list_length(elemlist);
    ptr = config->slot_names;
    foreach_ptr(char, slot_name, elemlist)
    {
        strcpy(ptr, slot_name);
        ptr += strlen(slot_name) + 1;
    }

    *extra = (void *) config;

    // Cleanup temporary allocations
    pfree(rawname);
    list_free(elemlist);
    return true;
}
```