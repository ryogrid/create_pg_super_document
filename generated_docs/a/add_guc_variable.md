# add_guc_variable

## Location
src/backend/utils/misc/guc.c: 1049 - 1077

## Overview
Adds a new GUC variable to the global hash table of known configuration variables, with automatic hash table expansion as needed.

## Definition
```c
static bool add_guc_variable(struct config_generic *var, int elevel)
```

## Detailed Description
This internal function is responsible for registering a new configuration variable in PostgreSQL's GUC hash table. It performs a hash table insertion operation using the variable's name as the key. The function is designed to handle memory allocation failures gracefully by reporting errors at the specified error level and returning a boolean status.

The function uses HASH_ENTER_NULL mode for hash_search, which allows it to detect and handle out-of-memory conditions during hash table expansion. If the insertion is successful, it stores a pointer to the config_generic structure in the hash entry for efficient lookup operations.

## Parameters / Member Variables
- `var`: Pointer to the config_generic structure representing the GUC variable to be added
- `elevel`: Error level to use when reporting out-of-memory conditions (e.g., ERROR, FATAL, WARNING)

## Dependencies
- Functions called/Symbols referenced:
  - hash_search
  - ereport
  - errcode
  - errmsg
- Data structures used:
  - config_generic
  - GUCHashEntry
  - HASH_ENTER_NULL
- Called from:
  - add_placeholder_variable (src/backend/utils/misc/guc.c:1211)
  - define_custom_variable (src/backend/utils/misc/guc.c:4962)

## Notes and Other Information
- This is a static function, only accessible within the guc.c file
- The function assumes that the variable being added does not already exist in the hash table (Assert(!found))
- Returns true on successful insertion, false on out-of-memory conditions
- The hash table automatically expands when needed during the HASH_ENTER_NULL operation
- Error reporting allows callers to choose appropriate error handling based on context (e.g., during initialization vs. runtime)