# set_extra_field

## Location
[src/backend/utils/misc/guc.c:794-813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L794-L813)

## Overview
A static utility function in PostgreSQL's GUC system that safely assigns a new 'extra' data structure to a field of a GUC configuration item, handling memory management by freeing the previous value if it's no longer referenced.

## Definition

```c
static void
set_extra_field(struct config_generic *gconf, void **field, void *newval)
```
## Detailed Description
The  function provides a memory-safe way to update 'extra' data fields within PostgreSQL's GUC configuration management system. It performs assignment of a new extra data pointer to a specified field while ensuring proper cleanup of the previous value. The function uses  to check if the old value is still referenced elsewhere in the configuration structure (including stacked states) before freeing it, preventing memory leaks while avoiding premature deallocation of shared extra data.

This function is part of the internal GUC infrastructure that manages additional data associated with configuration parameters. Extra data structures can contain type-specific information, validation functions, or other auxiliary data needed for parameter management.

## Parameters / Member Variables
- `*gconf`: Pointer to the generic GUC configuration structure containing the field to be updated
- `**field`: Pointer to the void* field that will be updated with the new extra data
- `*newval`: The new extra data pointer to assign to the field
## Dependencies
- Functions called/Symbols referenced:
  - [config_generic](../c/config_generic.md) (structure type)
  - [extra_field_used](../e/extra_field_used.md) (checks if old value is still referenced)
  - [guc_free](../g/guc_free.md) (frees memory allocated for old extra data)
- Called from (representative examples):
  - [set_stack_value](set_stack_value.md)
  - [discard_stack_value](../d/discard_stack_value.md)
  - [ResetAllOptions](../R/ResetAllOptions.md)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md)
  - newval (in configuration validation and assignment contexts)

## Notes and Other Information
- This is a static function, only accessible within src/backend/utils/misc/guc.c
- Provides memory-safe management of auxiliary data structures associated with GUC parameters
- Works in conjunction with  to ensure proper reference counting
- Part of PostgreSQL's sophisticated configuration parameter management system
- Essential for preventing memory leaks when updating extra data fields

## Simplified Source

```c
static void
set_extra_field(struct config_generic *gconf, void **field, void *newval)
{
    void *oldval = *field;

    // Assign the new value
    *field = newval;

    // Free old value if it exists and isn't referenced elsewhere
    if (oldval && !extra_field_used(gconf, oldval))
        guc_free(oldval);
}
```