# extra_field_used

## Location
[src/backend/utils/misc/guc.c:749-793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L749-L793)

## Overview
A static utility function in PostgreSQL's GUC system that checks whether a specific 'extra' data structure is referenced anywhere within a GUC configuration item, including current values, reset values, and stacked states.

## Definition

```c
static bool
extra_field_used(struct config_generic *gconf, void *extra)
```
## Detailed Description
The  function provides reference tracking for 'extra' data structures associated with GUC configuration parameters. It performs a comprehensive search to determine if a given extra data pointer is still being used anywhere within the configuration item, including the current extra field, reset_extra fields for all GUC variable types, and any extra fields in the configuration's stack of previous states.

This function is essential for memory management in the GUC system, ensuring that extra data structures are not prematurely freed while still being referenced. The function handles all GUC variable types (boolean, integer, real, string, enum) and traverses the entire stack of configuration states to check for references.

## Parameters / Member Variables
- `*gconf`: Pointer to the generic GUC configuration structure to search within
- `*extra`: Pointer to the extra data structure to check for references
## Dependencies
- Functions called/Symbols referenced:
  - [config_generic](../c/config_generic.md) (structure type)
  - [GucStack](../G/GucStack.md) (stack structure for tracking configuration states)
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM (GUC variable type constants)
  - config_bool, config_int, config_real, config_string, config_enum (type-specific structures)
- Called from (representative examples):
  - [set_extra_field](../s/set_extra_field.md)
  - newval (in configuration validation contexts)

## Notes and Other Information
- This is a static function, only accessible within src/backend/utils/misc/guc.c
- Performs exhaustive reference checking across all GUC variable types and stack levels
- Essential for preventing memory leaks and ensuring safe deallocation of extra data
- Part of PostgreSQL's sophisticated GUC parameter management system
- The function traverses both the current configuration state and the entire stack of previous states

## Simplified Source

```c
static bool extra_field_used(struct config_generic *gconf, void *extra)
{
    GucStack *stack;

    // Check if extra matches current extra field
    if (extra == gconf->extra)
        return true;

    // Check reset_extra field for each GUC variable type
    switch (gconf->vartype)
    {
        case PGC_BOOL:
            if (extra == ((struct config_bool *) gconf)->reset_extra)
                return true;
            break;
        case PGC_INT:
            if (extra == ((struct config_int *) gconf)->reset_extra)
                return true;
            break;
        case PGC_REAL:
            if (extra == ((struct config_real *) gconf)->reset_extra)
                return true;
            break;
        case PGC_STRING:
            if (extra == ((struct config_string *) gconf)->reset_extra)
                return true;
            break;
        case PGC_ENUM:
            if (extra == ((struct config_enum *) gconf)->reset_extra)
                return true;
            break;
    }

    // Check all entries in the configuration stack
    for (stack = gconf->stack; stack; stack = stack->prev)
    {
        if (extra == stack->prior.extra || extra == stack->masked.extra)
            return true;
    }

    return false;
}
```