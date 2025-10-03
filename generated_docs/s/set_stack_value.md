# set_stack_value

## Location
[src/backend/utils/misc/guc.c:814-847](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L814-L847)

## Overview
A static utility function in PostgreSQL's GUC system that copies a GUC variable's current active value into a stack entry, supporting all GUC variable types and handling associated extra data.

## Definition

```c
static void
set_stack_value(struct config_generic *gconf, config_var_value *val)
```
## Detailed Description
The  function is responsible for copying the current active value of a GUC configuration parameter into a stack entry structure. This is essential for PostgreSQL's configuration stack management, which allows parameters to be saved and restored across different contexts (such as transaction boundaries, function calls, or nested configuration scopes). The function handles all supported GUC variable types (boolean, integer, real, string, enum) and ensures that associated extra data is properly copied as well.

For string variables, the function uses  to ensure proper memory management and reference counting. After copying the type-specific value, it uses  to copy any associated extra data structure, maintaining consistency between the value and its metadata.

## Parameters / Member Variables
- `*gconf`: Pointer to the generic GUC configuration structure whose value will be copied
- `*val`: Pointer to the config_var_value structure that will receive the copied value and extra data
## Dependencies
- Functions called/Symbols referenced:
  - [config_var_value](../c/config_var_value.md), config_generic (structure types)
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM (GUC variable type constants)
  - config_bool, config_int, config_real, config_string, config_enum (type-specific structures)
  - [set_string_field](set_string_field.md) (for string value copying with memory management)
  - [set_extra_field](set_extra_field.md) (for extra data copying with memory management)
- Called from (representative examples):
  - [push_old_value](../p/push_old_value.md) (when saving current values before changes)

## Notes and Other Information
- This is a static function, only accessible within src/backend/utils/misc/guc.c
- Essential for PostgreSQL's configuration stack management and transaction-safe parameter changes
- The function requires that stringval and extra fields in the target structure be initialized to NULL before calling
- Handles all GUC variable types through a comprehensive switch statement
- Works in conjunction with memory management functions to ensure proper reference counting
- Part of the infrastructure that enables nested configuration scopes and rollback capabilities

## Simplified Source

```c
static void
set_stack_value(struct config_generic *gconf, config_var_value *val)
{
    // Copy the current value based on variable type
    switch (gconf->vartype) {
        case PGC_BOOL:
            val->val.boolval = *((struct config_bool *) gconf)->variable;
            break;
        case PGC_INT:
            val->val.intval = *((struct config_int *) gconf)->variable;
            break;
        case PGC_REAL:
            val->val.realval = *((struct config_real *) gconf)->variable;
            break;
        case PGC_STRING:
            // Use helper function for proper string memory management
            set_string_field((struct config_string *) gconf,
                             &(val->val.stringval),
                             *((struct config_string *) gconf)->variable);
            break;
        case PGC_ENUM:
            val->val.enumval = *((struct config_enum *) gconf)->variable;
            break;
    }

    // Copy associated extra data
    set_extra_field(gconf, &(val->extra), gconf->extra);
}
```