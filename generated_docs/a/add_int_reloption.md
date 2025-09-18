# add_int_reloption

## Location
src/backend/access/common/reloptions.c: 901 - 917

## Overview
A public function that creates and registers a new integer-type relation option (reloption) with the PostgreSQL reloptions system.

## Definition
```c
void
add_int_reloption(bits32 kinds, const char *name, const char *desc, int default_val,
                  int min_val, int max_val, LOCKMODE lockmode)
```

## Detailed Description
This function provides the public interface for adding integer-type relation options to PostgreSQL's reloptions system. It serves as a wrapper that combines the initialization and registration steps: first calling `init_int_reloption` to create and configure the option structure, then calling `add_reloption` to register it with the global reloptions catalog. Once registered, the option becomes available for use in SQL DDL statements like CREATE TABLE and ALTER TABLE with the WITH clause.

## Parameters / Member Variables
- `kinds`: A bitmask specifying which relation kinds (table, index, view, etc.) this option applies to
- `name`: The name of the reloption as it will appear in SQL statements
- `desc`: A human-readable description of the option for documentation and help systems
- `default_val`: The default integer value used when the option is not explicitly specified
- `min_val`: The minimum allowed integer value for validation
- `max_val`: The maximum allowed integer value for validation
- `lockmode`: The lock mode required when changing this option on an existing relation

## Dependencies
- Functions called/Symbols referenced:
  - init_int_reloption
  - add_reloption
  - relopt_gen
- Called from (representative examples):
  - create_reloptions_table
  - Various extension and index access method initialization functions

## Notes and Other Information
- This is the main public API for registering integer reloptions, typically called during server startup or extension loading
- The function follows PostgreSQL's two-phase pattern: initialize then register
- Options registered via this function become part of the global reloptions catalog and are available system-wide
- Extensions and custom access methods commonly use this function to define their configurable parameters
- The function assumes the caller has validated the parameter ranges and naming conventions