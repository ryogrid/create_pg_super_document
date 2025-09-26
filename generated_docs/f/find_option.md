# find_option

## Location
src/backend/utils/misc/guc.c: 1237 - 1289

## Overview
Looks up a GUC configuration parameter by name and returns its configuration record, with support for creating placeholders for custom variables and handling obsolete parameter names.

## Definition
```c
struct config_generic *find_option(const char *name, bool create_placeholders, bool skip_errors, int elevel)
```

## Detailed Description
This is the primary function for locating GUC configuration parameters in PostgreSQL. It performs a comprehensive lookup process:

1. **Hash Table Search**: First attempts to find the parameter in the main GUC hash table (`guc_hashtab`)
2. **Obsolete Name Mapping**: If not found, checks if the name is an obsolete name for a currently supported parameter using `map_old_guc_names`
3. **Placeholder Creation**: For custom variables (when `create_placeholders` is true), validates the name format and creates a placeholder if valid
4. **Error Handling**: Provides flexible error reporting based on `skip_errors` and `elevel` parameters

The function supports PostgreSQL's extensible configuration system by allowing custom variables (prefixed with module names) and maintaining backward compatibility with renamed parameters.

## Parameters / Member Variables
- `name`: The name of the GUC parameter to find (case-insensitive)
- `create_placeholders`: If true, create placeholder entries for valid custom variable names that don't exist yet
- `skip_errors`: If true, return NULL silently for unrecognized names instead of reporting errors
- `elevel`: Error level for reporting problems (ERROR, WARNING, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - `hash_search` - Searches the GUC hash table using HASH_FIND
  - `guc_name_compare` - Compares GUC names (case-insensitive)
  - `assignable_custom_variable_name` - Validates custom variable name format
  - `add_placeholder_variable` - Creates placeholder for custom variables
  - `ereport` - Reports configuration parameter errors
- Data structures used:
  - `GUCHashEntry` - Hash table entry containing GUC variable pointer
  - `config_generic` - Base configuration structure for all GUC types
  - `map_old_guc_names` - Array mapping obsolete names to current names
- Called from (representative examples):
  - `set_config_with_handle` - Sets configuration parameter values
  - `GetConfigOption` - Retrieves current parameter values
  - `GetConfigOptionFlags` - Gets parameter flags and metadata
  - `SelectConfigFiles` - Configuration file processing
  - `GUCArrayAdd`/`GUCArrayDelete` - Array parameter manipulation

## Notes and Other Information
- The function is recursive when handling obsolete parameter names - it calls itself with the current name
- Custom variables must follow the format `module_name.variable_name` to be considered valid
- Placeholder creation is essential for extensions that define custom GUC parameters at runtime
- The hash table lookup is O(1), while obsolete name checking uses linear search (acceptable due to small `map_old_guc_names` array)
- Internal errors (like out-of-memory during placeholder creation) always result in error reports regardless of `skip_errors`
- This function is central to PostgreSQL's configuration system and is used throughout the backend for parameter access