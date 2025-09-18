# add_real_reloption

## Location
[src/backend/access/common/reloptions.c:954-971](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L954-L971)

## Overview
A public function that creates and registers a new real (floating-point) type relation option (reloption) with the PostgreSQL reloptions system.

## Definition
```c
void
add_real_reloption(bits32 kinds, const char *name, const char *desc,
                   double default_val, double min_val, double max_val,
                   LOCKMODE lockmode)
```

## Detailed Description
This function provides the public interface for adding real (double-precision floating-point) type relation options to PostgreSQL's reloptions system. It serves as a wrapper that combines the initialization and registration steps: first calling `init_real_reloption` to create and configure the option structure, then calling `add_reloption` to register it with the global reloptions catalog. Once registered, the option becomes available for use in SQL DDL statements like CREATE TABLE and ALTER TABLE with the WITH clause. This function is the floating-point counterpart to `add_int_reloption`, used for options that require fractional or decimal values.

## Parameters / Member Variables
- `kinds`: A bitmask specifying which relation kinds (table, index, view, etc.) this option applies to
- `name`: The name of the reloption as it will appear in SQL statements
- `desc`: A human-readable description of the option for documentation and help systems
- `default_val`: The default double-precision floating-point value used when the option is not explicitly specified
- `min_val`: The minimum allowed double value for validation
- `max_val`: The maximum allowed double value for validation
- `lockmode`: The lock mode required when changing this option on an existing relation

## Dependencies
- Functions called/Symbols referenced:
  - [init_real_reloption](../i/init_real_reloption.md)
  - [add_reloption](add_reloption.md)
  - [relopt_gen](../r/relopt_gen.md)
- Called from (representative examples):
  - [create_reloptions_table](../c/create_reloptions_table.md)
  - Various extension and index access method initialization functions

## Notes and Other Information
- This is the main public API for registering real-valued reloptions, typically called during server startup or extension loading
- The function follows PostgreSQL's two-phase pattern: initialize then register
- Options registered via this function become part of the global reloptions catalog and are available system-wide
- Commonly used for percentage values, ratios, threshold values, and other parameters requiring fractional precision
- Extensions and custom access methods use this function to define configurable floating-point parameters
- The function assumes the caller has validated the parameter ranges and naming conventions