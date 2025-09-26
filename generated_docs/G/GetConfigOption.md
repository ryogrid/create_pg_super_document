# GetConfigOption

## Location
[src/backend/utils/misc/guc.c:4358-4407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L4358-L4407)

## Overview
Retrieves the current value of a configuration option as a string with optional privilege checking and error handling.

## Definition

```c
struct config_generic *record;
```
## Detailed Description
This function provides a public interface to retrieve the current value of any PostgreSQL configuration parameter as a string representation. It handles all GUC parameter types (boolean, integer, real, string, enum) and converts them to appropriate string formats. The function includes security features to restrict access to privileged parameters and provides flexible error handling for missing parameters.

The function performs privilege checking when restrict_privileged is true, ensuring that only superusers and members of the pg_read_all_settings role can access GUC_SUPERUSER_ONLY variables. The returned string is valid until the next configuration-related function call and should not be modified by the caller.

## Parameters / Member Variables
- : Name of the configuration parameter to retrieve
- : If true, return NULL for non-existent parameters; if false, throw an error
- : If true, enforce privilege checks for sensitive parameters

## Dependencies
- Functions called/Symbols referenced:
  - [find_option](../f/find_option.md)
  - [ConfigOptionIsVisible](../C/ConfigOptionIsVisible.md)
  - [config_enum_lookup_by_value](../c/config_enum_lookup_by_value.md)
  - [config_generic](../c/config_generic.md), config_bool, config_int, config_real, config_string, config_enum
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md)
  - [be_tls_init](../b/be_tls_init.md)
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md)
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Returns a const char* that should not be modified and is only valid until the next configuration call
- Handles type conversion for all GUC parameter types to string representation
- [Boolean](../B/Boolean.md) values are returned as "on"/"off", numbers use standard formatting
- [String](../S/String.md) parameters return the actual value or empty string if NULL
- Enum parameters are converted to their string representation using lookup functions
- Uses a static buffer for numeric conversions (256 bytes)
- Security-conscious design with privilege checking for sensitive parameters
- Located in src/backend/utils/misc/guc.c:4358-4407