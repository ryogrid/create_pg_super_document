# is_conninfo_option

## Location
src/backend/foreign/foreign.c: 601 - 624

## Overview
Checks if a provided option is one of the libpq connection information options, with context-aware validation.

## Definition


## Detailed Description
The `is_conninfo_option` function is a static utility function that validates whether a given option name corresponds to a recognized libpq connection option. The function performs context-aware validation by checking both the option name and the catalog context (Oid) from which the option originated.

The function iterates through the `libpq_conninfo_options` array, which contains all valid libpq connection options along with their associated contexts. It returns true if both the option name matches and the context matches the expected context for that option. If context is 0, the context check is bypassed.

## Parameters / Member Variables
- `option`: C string containing the name of the option to validate
- `context`: Oid representing the catalog context of the option, or 0 if context validation should be skipped

## Dependencies
- Functions called/Symbols referenced:
  - `strcmp`: Standard C string comparison function
  - [ConnectionOption](../C/ConnectionOption.md): Structure type for connection option definitions
  - `libpq_conninfo_options`: Array of valid libpq connection options
- Called from (representative examples):
  - [postgresql_fdw_validator](../p/postgresql_fdw_validator.md): Uses this function to validate connection options

## Notes and Other Information
- This is a static function, only accessible within the foreign.c module
- The function provides context-sensitive validation, allowing the same option name to be valid in different contexts
- Used primarily by foreign data wrapper validators to ensure only valid libpq connection options are accepted
- The `libpq_conninfo_options` array is expected to be null-terminated (checked via `opt->optname`)
- Returns false immediately if no matching option is found after checking all entries
- Located in src/backend/foreign/foreign.c:601-624