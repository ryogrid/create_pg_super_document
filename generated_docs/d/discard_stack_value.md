# discard_stack_value

## Location
[src/backend/utils/misc/guc.c:848-873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L848-L873)

## Overview
A static utility function in PostgreSQL's GUC system that safely discards values stored in a stack entry, handling memory cleanup for string values and associated extra data.

## Definition

```c
static void
discard_stack_value(struct config_generic *gconf, config_var_value *val)
```
## Detailed Description
The  function is responsible for properly cleaning up values stored in GUC configuration stack entries that are no longer needed. This function is essential for PostgreSQL's configuration stack management, ensuring that memory is properly freed when stack entries are discarded during transaction rollbacks, scope exits, or other configuration cleanup operations.

The function handles different GUC variable types appropriately: for simple types (boolean, integer, real, enum), no special cleanup is needed as they are stored by value. For string types, it uses  with NULL to ensure proper memory management and reference counting. For all types, it clears any associated extra data using  with NULL.

## Parameters / Member Variables
- `*gconf`: Pointer to the generic GUC configuration structure associated with the stack entry
- `*val`: Pointer to the config_var_value structure whose values will be discarded and cleaned up
## Dependencies
- Functions called/Symbols referenced:
  - [config_var_value](../c/config_var_value.md), config_generic (structure types)
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM (GUC variable type constants)
  - [config_string](../c/config_string.md) (string-specific structure)
  - [set_string_field](../s/set_string_field.md) (for string value cleanup with memory management)
  - [set_extra_field](../s/set_extra_field.md) (for extra data cleanup with memory management)
- Called from (representative examples):
  - [push_old_value](../p/push_old_value.md) (when discarding old values during stack operations)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md) (during end-of-transaction cleanup)

## Notes and Other Information
- This is a static function, only accessible within src/backend/utils/misc/guc.c
- Essential for preventing memory leaks in PostgreSQL's configuration stack management
- Handles different GUC variable types with appropriate cleanup strategies
- Works in conjunction with memory management functions to ensure proper reference counting
- Part of the infrastructure that enables safe rollback of configuration changes
- Only string and extra data fields require active cleanup; other types are cleaned up automatically

## Simplified Source

```c
static void
discard_stack_value(struct config_generic *gconf, config_var_value *val)
{
    switch (gconf->vartype)
    {
        case PGC_BOOL:
        case PGC_INT:
        case PGC_REAL:
        case PGC_ENUM:
            // No cleanup needed for scalar types
            break;
        case PGC_STRING:
            // Free string value using proper memory management
            set_string_field((struct config_string *) gconf,
                           &(val->val.stringval),
                           NULL);
            break;
    }
    // Clear any extra data associated with this value
    set_extra_field(gconf, &(val->extra), NULL);
}
```