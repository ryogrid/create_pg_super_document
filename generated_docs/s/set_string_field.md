# set_string_field

## Location
[src/backend/utils/misc/guc.c:733-748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L733-L748)

## Overview
A static utility function in PostgreSQL's GUC (Grand Unified Configuration) system that safely assigns a new string value to a field of a string GUC configuration item, handling memory management by freeing the previous value if it's no longer referenced.

## Definition

```c
static void
set_string_field(struct config_string *conf, char **field, char *newval)
```
## Detailed Description
The  function provides a memory-safe way to update string fields within PostgreSQL's configuration management system. It performs assignment of a new string value to a specified field pointer while ensuring proper cleanup of the previous value. The function checks if the old value is still referenced elsewhere in the configuration structure (including stacked states) before freeing it, preventing memory leaks while avoiding premature deallocation of shared string values.

This function is part of the internal GUC (Grand Unified Configuration) infrastructure that manages PostgreSQL's configuration parameters. It's designed to handle the complexity of string parameter management where values may be shared across different configuration states or stack levels.

## Parameters / Member Variables
- `*conf`: Pointer to the config_string structure representing the string GUC configuration item
- `**field`: Pointer to the char* field that will be updated with the new value
- `*newval`: The new string value to assign to the field
## Dependencies
- Functions called/Symbols referenced:
  - [string_field_used](string_field_used.md) (checks if old value is still referenced)
  - [guc_free](../g/guc_free.md) (frees memory allocated for old string value)
- Called from (representative examples):
  - [set_stack_value](set_stack_value.md)
  - [discard_stack_value](../d/discard_stack_value.md)
  - [ResetAllOptions](../R/ResetAllOptions.md)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md)
  - [define_custom_variable](../d/define_custom_variable.md)

## Notes and Other Information
- This is a static function, only accessible within src/backend/utils/misc/guc.c
- The function ensures memory safety by checking references before freeing old values
- Part of PostgreSQL's sophisticated configuration parameter management system
- Handles the complexity of shared string values across different GUC states and stack levels

## Simplified Source

```c
static void
set_string_field(struct config_string *conf, char **field, char *newval)
{
    char *oldval = *field;

    // Assign the new value
    *field = newval;

    // Free old value if it exists and isn't referenced elsewhere
    if (oldval && !string_field_used(conf, oldval))
        guc_free(oldval);
}
```