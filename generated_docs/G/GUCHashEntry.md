# GUCHashEntry

## Location
src/backend/utils/misc/guc.c: 212 - 261

## Overview
GUCHashEntry is a hash table entry structure that serves as a wrapper for PostgreSQL's Grand Unified Configuration (GUC) variables in the global hash table used for efficient GUC variable lookup by name.

## Definition


## Detailed Description
GUCHashEntry is a simple two-field structure that acts as an entry in PostgreSQL's GUC hash table (). This structure is designed to enable fast lookup of configuration variables by name. The hash table contains all GUC variables in the system, providing O(1) average-case access time for configuration parameter lookups.

The structure follows the dynahash API requirements by storing the hash key () separately from the actual data pointer (), even though this creates some redundancy since  contains the same information. This design choice accommodates the constraints of PostgreSQL's dynahash implementation which requires explicit key storage.

The hash table using these entries is created during GUC system initialization and remains active throughout the server's lifetime, supporting configuration parameter operations like SET, SHOW, and configuration file processing.

## Parameters / Member Variables
- : A string containing the name of the GUC variable, used as the hash key for lookups. This is redundant with  but required by the dynahash API.
- : A pointer to the actual  structure that contains the complete definition and current state of the GUC variable, including its value, source, context, and other metadata.

## Dependencies
- Functions called/Symbols referenced:
  - HTAB (hash table type for )
  - config_generic (the actual GUC variable structure)
  - dlist_head, slist_head (for additional GUC categorization lists)
  - guc_name_hash, guc_name_match (hash table operations)

- Called from (representative examples):
  - get_guc_variables (iterates through hash table entries)
  - build_guc_variables (constructs hash entries during initialization)
  - add_guc_variable (creates new hash entries for custom GUCs)
  - find_option (looks up GUC variables by name)
  - InitializeGUCOptions (populates the hash table during startup)
  - define_custom_variable (adds custom GUC variables to the hash table)

## Notes and Other Information
- The hash table storing these entries () is static and private to the GUC subsystem
- Besides the main hash table, GUC variables are also organized into specialized linked lists (non-default values, stacked values, and variables needing reporting) for efficient category-based operations
- The redundant storage of the GUC name is a compromise required by PostgreSQL's dynahash implementation
- This structure is only used internally within the GUC system and is not exposed to external modules
- The hash table provides the primary mechanism for translating string-based configuration parameter names into their corresponding internal structures during SQL operations and configuration file processing