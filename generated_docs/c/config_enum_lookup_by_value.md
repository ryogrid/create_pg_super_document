# config_enum_lookup_by_value

## Location
[src/backend/utils/misc/guc.c:3025-3047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L3025-L3047)

## Overview
Performs reverse lookup to find the string name corresponding to a given integer value in a PostgreSQL configuration enum.

## Definition
```c
const char *config_enum_lookup_by_value(struct config_enum *record, int val)
```

## Detailed Description
This function searches through a configuration enum's option table to find the string name that corresponds to a given integer value. It's used primarily for displaying enum configuration values in human-readable form, such as converting internal numeric representations back to their string equivalents for user display or logging.

The function expects to be called only with valid values that are known to exist in the enum. If the requested value is not found, it throws an ERROR using elog(), indicating a programming error or data corruption rather than user input error.

The returned string pointer references static data within the enum definition and should not be modified or freed by the caller.

## Parameters / Member Variables
- `record`: Pointer to the config_enum structure containing the enum definition and options table
- `val`: The integer value to look up in the enum options

## Dependencies
- Functions called/Symbols referenced:
  - config_enum (struct type)
  - config_enum_entry (struct type)
  - elog (PostgreSQL logging/error function)
- Called from (representative examples):
  - GetConfigOption
  - GetConfigOptionResetString
  - ShowGUCOption
  - write_one_nondefault_variable
  - estimate_variable_size
  - serialize_variable
  - call_enum_check_hook
  - GetConfigOptionValues
  - printMixedStruct

## Notes and Other Information
- Should only be called with known-valid enum values
- Throws ERROR if the value is not found, indicating a programming error
- Returns a pointer to static string data that should not be modified
- Used extensively throughout the GUC system for displaying enum values
- Part of PostgreSQL's configuration system infrastructure for enum-type parameters
- The search is linear through the options array until a matching value is found