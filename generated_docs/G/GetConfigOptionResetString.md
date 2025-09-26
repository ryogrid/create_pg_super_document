# GetConfigOptionResetString

## Location
[src/backend/utils/misc/guc.c:4408-4454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L4408-L4454)

## Overview
Returns the RESET value associated with a specified PostgreSQL configuration option, formatted as a string representation suitable for display or logging purposes.

## Definition

```c
struct config_generic *record;
```
## Detailed Description
This function retrieves the reset value (default value) of a PostgreSQL configuration parameter identified by name. The reset value represents the value that the parameter would have if it were reset to its default state, either through RESET statement or server restart. The function handles different parameter types (boolean, integer, real, string, enum) and converts their reset values to appropriate string representations.

The function performs permission checks to ensure only authorized users can examine configuration parameters, specifically requiring privileges of the "pg_read_all_settings" role for restricted parameters.

Note: This function is not re-entrant due to its use of a static result buffer for numeric values. The returned string pointer should be used immediately or copied, as subsequent calls may overwrite the buffer contents.

## Parameters / Member Variables
- : The name of the configuration parameter whose reset value is to be retrieved

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