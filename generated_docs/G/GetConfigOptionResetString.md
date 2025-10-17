# GetConfigOptionResetString

## Location
[src/backend/utils/misc/guc.c:4408-4454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L4408-L4454)

## Overview
Returns the RESET value associated with a specified PostgreSQL configuration option, formatted as a string representation suitable for display or logging purposes.

## Definition

```c
const char *GetConfigOptionResetString(const char *name)
```
## Detailed Description
This function retrieves the reset value (default value) of a PostgreSQL configuration parameter identified by name. The reset value represents the value that the parameter would have if it were reset to its default state, either through RESET statement or server restart. The function handles different parameter types (boolean, integer, real, string, enum) and converts their reset values to appropriate string representations.

The function performs permission checks to ensure only authorized users can examine configuration parameters, specifically requiring privileges of the "pg_read_all_settings" role for restricted parameters.

Note: This function is not re-entrant due to its use of a static result buffer for numeric values. The returned string pointer should be used immediately or copied, as subsequent calls may overwrite the buffer contents.

## Parameters / Member Variables
- `name`: The name of the configuration parameter whose reset value is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [find_option](../f/find_option.md)
  - [ConfigOptionIsVisible](../C/ConfigOptionIsVisible.md)
  - [config_enum_lookup_by_value](../c/config_enum_lookup_by_value.md)
  - snprintf
  - ereport
- Data structures used:
  - [config_generic](../c/config_generic.md)
  - config_bool
  - [config_int](../c/config_int.md)
  - [config_real](../c/config_real.md)
  - [config_string](../c/config_string.md)
  - [config_enum](../c/config_enum.md)
- Constants referenced:
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM
- Called from (representative examples):
  - [check_datestyle](../c/check_datestyle.md)
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Uses a static 256-byte buffer for formatting numeric values, making the function non-reentrant
- Returns "on"/"off" for boolean parameters, numeric strings for int/real parameters, and original strings for string/enum parameters
- Throws ERROR if parameter is not visible to current user due to insufficient privileges
- Returns empty string for NULL string parameters
- The returned pointer's validity is limited and should not be assumed to persist across multiple calls

## Simplified Source

```c
const char *GetConfigOptionResetString(const char *name) {
    struct config_generic *record;
    static char buffer[256];

    // Find the configuration option
    record = find_option(name, false, false, ERROR);
    Assert(record != NULL);

    // Check visibility permissions
    if (!ConfigOptionIsVisible(record))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied to examine \"%s\"", name),
                       errdetail("Only roles with privileges of the \"%s\" role may examine this parameter.",
                                "pg_read_all_settings")));

    // Return reset value based on parameter type
    switch (record->vartype) {
        case PGC_BOOL:
            return ((struct config_bool *) record)->reset_val ? "on" : "off";

        case PGC_INT:
            snprintf(buffer, sizeof(buffer), "%d",
                    ((struct config_int *) record)->reset_val);
            return buffer;

        case PGC_REAL:
            snprintf(buffer, sizeof(buffer), "%g",
                    ((struct config_real *) record)->reset_val);
            return buffer;

        case PGC_STRING:
            return ((struct config_string *) record)->reset_val ?
                   ((struct config_string *) record)->reset_val : "";

        case PGC_ENUM:
            return config_enum_lookup_by_value((struct config_enum *) record,
                                             ((struct config_enum *) record)->reset_val);
    }
    return NULL;
}
```