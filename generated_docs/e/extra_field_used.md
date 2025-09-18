# extra_field_used

## Location
src/backend/utils/misc/guc.c: 749 - 793

## Overview
A static utility function in PostgreSQL's GUC system that checks whether a specific 'extra' data structure is referenced anywhere within a GUC configuration item, including current values, reset values, and stacked states.

## Definition


## Detailed Description
The  function provides reference tracking for 'extra' data structures associated with GUC configuration parameters. It performs a comprehensive search to determine if a given extra data pointer is still being used anywhere within the configuration item, including the current extra field, reset_extra fields for all GUC variable types, and any extra fields in the configuration's stack of previous states.

This function is essential for memory management in the GUC system, ensuring that extra data structures are not prematurely freed while still being referenced. The function handles all GUC variable types (boolean, integer, real, string, enum) and traverses the entire stack of configuration states to check for references.

## Parameters / Member Variables
- : Pointer to the generic GUC configuration structure to search within
- : Pointer to the extra data structure to check for references

## Dependencies
- Functions called/Symbols referenced:
  - config_generic (structure type)
  - GucStack (stack structure for tracking configuration states)
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM (GUC variable type constants)
  - config_bool, config_int, config_real, config_string, config_enum (type-specific structures)
- Called from (representative examples):
  - [set_extra_field](../s/set_extra_field.md)
  - newval (in configuration validation contexts)

## Notes and Other Information
- This is a static function, only accessible within src/backend/utils/misc/guc.c
- Performs exhaustive reference checking across all GUC variable types and stack levels
- Essential for preventing memory leaks and ensuring safe deallocation of extra data
- Part of PostgreSQL's sophisticated GUC parameter management system
- The function traverses both the current configuration state and the entire stack of previous states