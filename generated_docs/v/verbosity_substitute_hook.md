# verbosity_substitute_hook

## Location
src/bin/psql/startup.c: 1119 - 1126

## Overview
A variable substitute hook function that provides a default value for the VERBOSITY variable in psql when no value is specified or when the value is NULL.

## Definition


## Detailed Description
This substitute hook function is called before the main verbosity_hook when the VERBOSITY variable is being set or accessed. Its primary purpose is to ensure that the VERBOSITY variable always has a valid value by providing "default" as the fallback when NULL is passed. This prevents the main verbosity_hook from having to handle NULL values and ensures consistent behavior. The function uses pg_strdup to allocate memory for the default string.

## Parameters / Member Variables
- : The proposed new value for the VERBOSITY variable. If NULL, will be replaced with "default".

## Dependencies
- Functions called/Symbols referenced:
  - pg_strdup (PostgreSQL string duplication function)
- Called from (representative examples):
  - SetVariableHooks registration in EstablishVariableSpace (as substitute hook for VERBOSITY)

## Notes and Other Information
- This is a substitute hook, which runs before the main hook (verbosity_hook) and can modify the value before it's processed
- The function ensures that verbosity_hook never receives NULL by substituting "default" as the fallback value
- Located in src/bin/psql/startup.c:1119
- The returned string is dynamically allocated and should be freed appropriately by the calling context